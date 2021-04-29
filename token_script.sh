#!/usr/bin/env bash

solc --abi erc20.sol
./builds/bin/abigen --abi=erc20_sol_ERC20.abi --pkg=token --out=erc20.go

