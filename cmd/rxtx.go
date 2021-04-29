/*
Copyright © 2019 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/influxdata/influxdb/pkg/deep"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
	"golang.org/x/crypto/sha3"
)

type txn struct {
	txHash common.Hash
	tx     *types.Transaction
}

var (
	baseRPCEndpoint       string = "http://localhost:8545"
	targetRPCEndpoint     string = "http://localhost:8546"
	targetChainConfigFile string = "target_config.json"
	keyDir                string = "keys/"
	signerAddress         string = "0x"
	passFile              string = "pass.txt"
	password              string
	lightSigning          bool
	dbDir                 string = filepath.Join(os.Getenv("HOME"), ".extx")
	logVerbose            bool

	//queue *goque.PriorityQueue

	// Holds queue by blocknumber:transaction index:{job}
)

type clientManager struct {
	ctx     context.Context
	cancel  context.CancelFunc
	client  *ethclient.Client
	chainid *big.Int
}

type signingAccount struct {
	acc     accounts.Account
	balance *big.Int
	nonce   *atomic.Uint64
}

func (s signingAccount) String() string {
	balE := new(big.Int).Div(s.balance, new(big.Int).SetUint64(uint64(params.Ether)))
	return fmt.Sprintf(`Account: %x
File: %s
Balance: %v
Nonce: %v`, s.acc.Address, s.acc.URL, balE, s.nonce.Load())
}

type DictItem struct {
	BaseTx     common.Hash
	TargetTx   common.Hash
	BaseAddr   common.Address
	TargetAddr common.Address
}
type job struct {
	txHash common.Hash
	tx     *types.Transaction
	blockN uint64
	txI    uint64
}

// rxtxCmd represents the rxtx command
var rxtxCmd = &cobra.Command{
	Use:   "rxtx",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {

		var err error

		cmd.Flags().Parse(args)

		// DBs
		//queue, err = goque.OpenPriorityQueue(filepath.Join(dbDir, "q"), goque.ASC)
		//if err != nil {
		//	log.Fatal(err)
		//}
		//defer queue.Close()

		db, err := leveldb.New(filepath.Join(dbDir, "store"), 512, 16, "extx")
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		// Clients
		var base, target = &clientManager{}, &clientManager{}
		if err := startClient(base, baseRPCEndpoint); err != nil {
			log.Fatal(err)
		}
		if err := startClient(target, targetRPCEndpoint); err != nil {
			log.Fatal(err)
		}

		// Target signing account
		signerAccount := signingAccount{}
		b, err := ioutil.ReadFile(passFile)
		if err != nil {
			log.Fatal(err)
		}
		password = strings.TrimSuffix(string(b), "\n")

		n, p := keystore.StandardScryptN, keystore.StandardScryptP
		if lightSigning {
			n, p = keystore.LightScryptN, keystore.LightScryptP
		}
		keyStore := keystore.NewKeyStore(keyDir, n, p)
		acc, err := keyStore.Find(accounts.Account{Address: common.HexToAddress(signerAddress)})
		if err != nil {
			log.Fatal(err)
		}
		signerAccount.acc = acc
		bal, err := target.client.BalanceAt(target.ctx, signerAccount.acc.Address, nil)
		if err != nil {
			log.Fatal(err)
		}
		signerAccount.balance = bal

		nonce, err := target.client.NonceAt(target.ctx, signerAccount.acc.Address, nil)
		if err != nil {
			log.Fatal(err)
		}
		signerAccount.nonce = atomic.NewUint64(nonce - 1) // sub 1; will increment PRIOR to inclusion in transaction.

		log.Println(signerAccount.String())

		// Account spending and limits
		maxSpend := new(big.Int).Div(bal, big.NewInt(2)) // max spend = 50% available balance
		minBalance := new(big.Int).Sub(bal, maxSpend)
		maxValue := new(big.Int).Mul(big.NewInt(10), new(big.Int).SetUint64(uint64(params.Ether)))

		var i uint64 = 0 // Iteration tally

		var blockN, tiN uint64
		var tx *types.Transaction
		var txHash common.Hash

		// Monitor account spending.
		// If account spending causes balance to drop below minBalance,
		// execution is halted abruptly.
		accountMonitorTicker := time.NewTicker(1 * time.Second)
		defer accountMonitorTicker.Stop()
		go func() {
			for {
				select {
				case t := <-accountMonitorTicker.C:
					if t.Second()%20 == 0 {
						bal2, err := target.client.BalanceAt(target.ctx, signerAccount.acc.Address, nil)
						if err != nil {
							log.Fatal(err)
						}
						if bal2.Cmp(minBalance) <= 0 {
							panic("spent max value")
						}
						signerAccount.balance = bal2
					} else if t.Second()%30 == 0 {
						log.Println("Scanned", i, "lines (", blockN, tiN, ")")
					}
				}
			}
		}()

		scanner := bufio.NewScanner(os.Stdin)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024*10)

		logV := func(args ...interface{}) {
			if logVerbose {
				log.Println(args...)
			}
		}

		var checkyCheckI int

		var lastLine uint64 = 1
		if v, err := db.Get([]byte("last")); err == nil && v != nil {
			lastLine, err = strconv.ParseUint(string(v), 10, 64)
			if err != nil {
				log.Fatalln(err)
			}
		}

		for scanner.Scan() {
			i++

			if i < lastLine {
				continue
			}

			err = db.Put([]byte("last"), []byte(fmt.Sprintf("%d", i)))
			if err != nil {
				log.Fatalln(err)
			}

			blockN, tiN, txHash, tx, err = parseTransactionLine(scanner.Bytes())
			if err != nil {
				log.Fatalln(err)
			}
			logV("Scanned block", blockN, "tx", tiN)
			if txHash == (common.Hash{}) {
				logV("Skipping empty transaction hash")
				continue
			}
			if tx.Data() == nil || len(tx.Data()) == 0 {
				logV("Skipping empty transaction data")
				continue
			}

			// Only look at any given transaction once. (indexed by hash)
			keySeen := append([]byte("seen"), txHash.Bytes()...)
			if _, err := db.Get(keySeen); err == nil {
				continue
			} else {
				err = db.Put(keySeen, nil)
				if err != nil {
					log.Fatalln(err)
				}
			}
			//// Redundant to the above.
			//if existing, err := db.Get(txHash.Bytes()); err == nil || existing != nil {
			//	logV("Skipping processed transaction", txHash.Hex())
			//	continue
			//}

		ploop:
			for {
				pending, err := target.client.PendingTransactionCount(target.ctx)
				if err != nil {
					log.Fatalln(err)
				}
				if pending > 256 {
					log.Println("Pending transactions > 256, sleeping 15s")
					time.Sleep(15 * time.Second)
				} else {
					break ploop
				}
			}

			var targetTo common.Address
			if tx.To() != nil {
				if *tx.To() == (common.Address{}) {
					logV("Skipping transaction to a black hole")
					continue
				}

				// Look up contract address pair
				targetToV, err := db.Get(tx.To().Bytes())
				if err != nil {
					// This is a contract call transaction for a contract which we have not handled.
					logV("Skipping transaction to unknown address")
					continue
				}
				targetTo = common.BytesToAddress(targetToV)
			}

			// Because I don't trust my unmarshaling. Get the canonical typed version.
			// FIXME
			if checkyCheckI < 1000 {
				var pending bool
				tx2, pending, err := base.client.TransactionByHash(base.ctx, txHash)
				if err != nil {
					log.Fatalln(err)
				}
				if pending {
					continue
				}
				if bytes.Compare(tx.Data(), tx2.Data()) != 0 {
					log.Println(deep.Equal(tx.Data(), tx2.Data()))
					log.Fatalln("different bytes wft")
				}
				checkyCheckI++
			}

			value := tx.Value()
			if value.Cmp(maxValue) > 0 {
				value = maxValue
			}

			if tx.To() == nil {

				// Skip duplicate contract deploys.
				if existing, err := db.Get(tx.Data()); err == nil || existing != nil {
					logV("Skipping duplicate contract creation")
					continue
				}

				receipt, err := base.client.TransactionReceipt(base.ctx, txHash)
				if err != nil {
					log.Fatalln("eth_getTransactionReceipt", err)
				}
				baseAddr := receipt.ContractAddress

				err = db.Put(baseAddr.Bytes(), nil)
				if err != nil {
					log.Fatalln(err)
				}

				gas := tx.Gas()

				//callMsg := ethereum.CallMsg{
				//	From: signerAccount.acc.Address,
				//	To:   nil,
				//	Data: tx.Data(),
				//}
				//
				//

				//gas, err := target.client.EstimateGas(target.ctx, callMsg)
				//if err != nil {
				//	gas = tx.Gas()
				//	//log.Println("estimate gas", err)
				//}

				gasPrice, err := target.client.SuggestGasPrice(target.ctx)
				if err != nil {
					gasPrice = tx.GasPrice()
					log.Println("suggest gas price", err)
				}

				n, err := target.client.PendingNonceAt(target.ctx, signerAccount.acc.Address)
				if err != nil {
					log.Fatalln(err)
				} else {
					signerAccount.nonce.Store(n)
				}

				newtx := types.NewContractCreation(signerAccount.nonce.Load(), value, gas, gasPrice, tx.Data())

				signedTx, err := keyStore.SignTxWithPassphrase(signerAccount.acc, password, newtx, target.chainid)
				if err != nil {
					log.Fatalln(err)
				}

				newContractAddress := crypto.CreateAddress(signerAccount.acc.Address, n)

				err = target.client.SendTransaction(target.ctx, signedTx)
				if err != nil {
					log.Println("eth_sendTransaction (create)", err, txHash, spew.Sdump(signedTx))
					continue
				}

				err = db.Put(txHash.Bytes(), signedTx.Hash().Bytes())
				if err != nil {
					log.Fatalln(err)
				}
				err = db.Put(signedTx.Hash().Bytes(), txHash.Bytes())
				if err != nil {
					log.Fatalln(err)
				}
				err = db.Put(signedTx.Data(), nil)
				if err != nil {
					log.Fatalln(err)
				}
				err = db.Put(baseAddr.Bytes(), newContractAddress.Bytes())
				if err != nil {
					log.Fatalln(err)
				}
				err = db.Put(newContractAddress.Bytes(), baseAddr.Bytes())
				if err != nil {
					log.Fatalln(err)
				}

				log.Printf("[%d / %d] Contract created: base.transaction=%s base.contract=%s target.transaction=%s target.contract=%s\n", blockN, tiN, txHash.Hex(), baseAddr.Hex(), signedTx.Hash().Hex(), newContractAddress.Hex())

				continue

			}

			// targetTo was not nil.

			//if isTokenStandardContract(target.client, target.ctx, signerAccount.acc.Address, tx) {
			//	log.Println("Skipping standard token contract call")
			//	continue
			//}

			key := append([]byte("tx2c"), tx.To().Bytes()...)

			// Skip duplicate contract call transactions (same data to same address).
			if existing, err := db.Get(key); err == nil && existing != nil && bytes.Compare(existing, tx.Data()) == 0 {
				logV("Skipping duplicate contract call")
				continue
			}

			// Only make the first 10 calls to any given contract.
			// We're not interested here in reproducing state, just testing probable use, aiming
			// for variance instead of consistency. This is assuming that the first 10 calls to a contract
			// are representative of all subsequent calls, and that they will include close to the full diversity
			// of interactions with that contract.
			counterKey := append([]byte("tx2cc"), tx.To().Bytes()...)
			if existing, err := db.Get(counterKey); err != nil && existing == nil {
				err = db.Put(counterKey, []byte("1"))
				if err != nil {
					log.Fatalln(err)
				}
			} else {
				s := string(existing)
				ii, err := strconv.Atoi(s)
				if err != nil {
					log.Fatalln(err)
				}
				if i > 9 {
					continue
				}
				ii++
				err = db.Put(counterKey, []byte(strconv.Itoa(ii)))
				if err != nil {
					log.Fatalln(err)
				}
			}

			//callMsg := ethereum.CallMsg{
			//	From: signerAccount.acc.Address,
			//	To:   nil,
			//	Data: tx.Data(),
			//}

			//gas, err := target.client.EstimateGas(target.ctx, callMsg)
			//if err != nil {
			//	gas = tx.Gas()
			//	//log.Println("estimate gas", err)
			//}

			gas := tx.Gas()

			gasPrice, err := target.client.SuggestGasPrice(target.ctx)
			if err != nil {
				gasPrice = tx.GasPrice()
				log.Println("suggest gas price", err)
			}

			//gas = gas * 15 / 10
			//gasPrice.Mul(gasPrice, big.NewInt(2))

			n, err := target.client.PendingNonceAt(target.ctx, signerAccount.acc.Address)
			if err != nil {
				log.Fatalln(err)
			} else {
				signerAccount.nonce.Store(n)
			}

			newtx := types.NewTransaction(signerAccount.nonce.Load(), targetTo, value, gas, gasPrice, tx.Data())

			signedTx, err := keyStore.SignTxWithPassphrase(signerAccount.acc, password, newtx, target.chainid)
			if err != nil {
				log.Fatalln(err)
			}

			err = target.client.SendTransaction(target.ctx, signedTx)
			if err != nil {
				log.Println("eth_sendTransaction (call)", err, txHash.Hex(), spew.Sdump(signedTx))
				continue
			}

			err = db.Put(txHash.Bytes(), signedTx.Hash().Bytes())
			if err != nil {
				log.Fatalln(err)
			}
			err = db.Put(key, tx.Data())
			if err != nil {
				log.Fatalln(err)
			}

			log.Printf("[%d / %d] Contract call: base.tx=%s target.tx=%s target.to=%s\n", blockN, tiN, txHash.Hex(), signedTx.Hash().Hex(), signedTx.To().Hex())
		}

		if scanner.Err() != nil {
			log.Println(scanner.Err())
		}
		log.Println("Done")
	},
}

// rxtxCmd represents the rxtx command
var inspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "Inspects key/value database",
	Long: `Use: $0 inspect <0xTxHash>`,
	Run: func(cmd *cobra.Command, args []string) {
		var err error

		cmd.Flags().Parse(args)

		if len(args) == 0 {
			log.Fatalln("Missing required ARG1=<0xTxHash>")
		}

		db, err := leveldb.New(filepath.Join(dbDir, "store"), 512, 16, "extx")
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		targetHash := args[0]

		fmt.Printf("TxHash: %+v\n", targetHash)

		th := common.HexToHash(targetHash)

		if val, err := db.Get(th.Bytes()); err == nil {
			fmt.Printf("%s\n", common.Bytes2Hex(val))
			return
		}
		iter  := db.NewIterator()
		for iter.Next() {
			if bytes.Equal(iter.Value(), th.Bytes()) {
				fmt.Printf("%v\n", common.Bytes2Hex(iter.Key()))
			}
		}

	},
}

func init() {
	rootCmd.AddCommand(rxtxCmd)
	rootCmd.AddCommand(inspectCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// rxtxCmd.PersistentFlags().String("foo", "", "A help for foo")
	rxtxCmd.Flags().StringVarP(&baseRPCEndpoint, "base-rpc-endpoint", "b", baseRPCEndpoint, "Base client RPC endpoint")
	rxtxCmd.Flags().StringVarP(&targetRPCEndpoint, "target-rpc-endpoint", "t", targetRPCEndpoint, "Target client RPC endpoint")
	rxtxCmd.Flags().StringVarP(&targetChainConfigFile, "chainspec", "c", targetChainConfigFile, "Chain config path (multigeth format)")
	rxtxCmd.Flags().StringVarP(&keyDir, "keydir", "k", keyDir, "Path to keys directory")
	rxtxCmd.Flags().StringVarP(&signerAddress, "signer", "s", signerAddress, "Signed address (must belong to key in key dir path).")
	rxtxCmd.Flags().StringVarP(&passFile, "pass-file", "p", passFile, "File containing account password.")
	rxtxCmd.Flags().BoolVarP(&lightSigning, "light", "l", false, "Light signing Scrypt parameters (default standard)")
	rxtxCmd.Flags().StringVarP(&dbDir, "datastore", "d", dbDir, "Base client RPC endpoint")
	rxtxCmd.Flags().BoolVarP(&logVerbose, "verbose", "v", false, "Enable verbose logging")

	inspectCmd.Flags().StringVarP(&dbDir, "datastore", "d", dbDir, "Base client RPC endpoint")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// rxtxCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func startClient(m *clientManager, endpoint string) error {
	ctx := context.Background()
	m.ctx = ctx

	c, err := ethclient.DialContext(ctx, endpoint)
	if err != nil {
		return err
	}
	m.client = c

	chainid, err := m.client.ChainID(m.ctx)
	if err != nil {
		return err
	}
	m.chainid = chainid
	return nil
}

// parseTransactionLine parses CSV formatted transaction values into a native transaction type.
// THE TRANSACTION WILL NOT BE EQUIVALENT TO THE NATIVE VERSION, eg. txHash != tx.Hash.
func parseTransactionLine(input []byte) (blockN, tiN uint64, txHash common.Hash, tx *types.Transaction, err error) {
	b := bytes.NewBuffer(input)
	reader := csv.NewReader(b)
	record, err := reader.Read()
	if err != nil {
		return 0, 0, txHash, nil, err
	}

	// strings
	hashS, nonceS, _, blockNumberS, transactionIndexS, _, toS, valueS, gasS, gasPriceS, inputS, _ := record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9], record[10], record[11]

	// Catch header line.
	if hashS == "hash" {
		return // no err, but empty values
	}

	blockN, err = strconv.ParseUint(blockNumberS, 10, 64)
	if err != nil {
		return blockN, tiN, txHash, tx, err
	}
	tiN, err = strconv.ParseUint(transactionIndexS, 10, 64)
	if err != nil {
		return blockN, tiN, txHash, tx, err
	}

	txHash = common.HexToHash(hashS)

	nonceU, err := strconv.ParseUint(nonceS, 10, 64)
	if err != nil {
		return blockN, tiN, txHash, nil, err
	}
	amount, ok := new(big.Int).SetString(valueS, 10)
	if !ok {
		return blockN, tiN, txHash, nil, errors.New("failed to parse value from transaction csv")
	}

	gas, err := strconv.ParseUint(gasS, 10, 64)
	if err != nil {
		return blockN, tiN, txHash, nil, err
	}

	gasPriceU, err := strconv.ParseUint(gasPriceS, 10, 64)
	if err != nil {
		return blockN, tiN, txHash, nil, err
	}
	gasPrice := new(big.Int).SetUint64(gasPriceU)

	if strings.HasPrefix(inputS, "0x") {
		inputS = strings.TrimPrefix(inputS, "0x")
	}
	data := common.Hex2Bytes(inputS)

	if len(toS) < 10 {
		tx = types.NewContractCreation(nonceU, amount, gas, gasPrice, data)
	} else {
		tx = types.NewTransaction(nonceU, common.HexToAddress(toS), amount, gas, gasPrice, data)
	}

	return blockN, tiN, txHash, tx, nil
}

func isTokenStandardContract(client *ethclient.Client, ctx context.Context, fromAddr common.Address, tx *types.Transaction) bool {
	signature := []byte("totalSupply()") // ERC-20

	hash := sha3.NewLegacyKeccak256()
	hash.Write(signature)
	methodID := hash.Sum(nil)[:4]
	//hex := hexutil.Encode(methodID)

	msg := ethereum.CallMsg{
		From:     fromAddr,
		To:       tx.To(),
		Gas:      tx.Gas(),
		GasPrice: tx.GasPrice(),
		Value:    tx.Value(),
		Data:     methodID,
	}

	ret, err := client.CallContract(ctx, msg, nil)
	if err != nil {
		log.Fatalln(err)
	}
	res := ret != nil && len(ret) > 0
	log.Println("Is token contract", res, ret, "(=)", common.Bytes2Hex(ret), tx.To().Hash().Hex())
	return res

	//signature = []byte("balanceOf(address)") // ERC-20, ERC-721
	//
	//hash := sha3.NewLegacyKeccak256()
	//hash.Write(signature)
	//methodID := hash.Sum(nil)[:4]
	//hex := hexutil.Encode(methodID)

}
