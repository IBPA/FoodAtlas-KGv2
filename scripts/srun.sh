#!/bin/bash

srun --time=02:00:00 --ntasks 1 --cpus-per-task 8 --gres gpu:1 --mem 8G --pty bash
