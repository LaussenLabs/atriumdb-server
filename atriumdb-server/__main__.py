#!/usr/bin/env python

import argparse
import logging

def main():
    print("""\033[0;34m
       _  _       _            ___  ___ 
      /_\| |_ _ _(_)_  _ _ __ |   \| _ )
     / _ \  _| '_| | || | '  \| |) | _ \\
    /_/ \_\__|_| |_|\_,_|_|_|_|___/|___/                               
    \033[0m.""")
    parser = argparse.ArgumentParser(prog="atriumdb-server", description="Launches atriumdb-server services")
    parser.add_argument(
        "-s", "--service", help="Service to start"
    )

    args = parser.parse_args()
    print("Starting Service(s): ")
    print(args.service)

if __name__ == "__main__":
    main()








