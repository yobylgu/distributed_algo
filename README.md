# CS4545 Project Repository

> You are only allowed to use one of the three templates for your project. Once you
> decide on a template, delete the other two folders from your repository.

This repository contains three templates, which you can choose from:

## in4150
This is the template used in previous years. It is written in python and
runs experiments in a docker network, with each node executed in a docker container.

## distbench-rs
This is the new framework written in Rust. It supports running experiments both
in a single process, by simulating network channels, or with messages passed
over TCP sockets.

It has been tested less than the in4150 template, so please report any issues
you encounter, either on brightspace discussions, or by opening an issue on
[github](https://github.com/arg3t/distbench)

## distbench-py
This is the python port of distbench-rs.
