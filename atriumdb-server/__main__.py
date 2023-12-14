"""
import argparse
parser = argparse.ArgumentParser(prog='atriumdb-server')
parser.add_argument('integers', metavar='N', type=int, nargs='+',
                    help='an integer for the accumulator')
parser.add_argument('--sum', dest='accumulate', action='store_const',
                    const=sum, default=max,
                    help='sum the integers (default: find the max)')

args = parser.parse_args()
print(args.accumulate(args.integers))
"""

print("""\033[0;34m
     _   _        _                 ____  ____  
    / \ | |_ _ __(_)_   _ _ __ ___ |  _ \| __ ) 
   / _ \| __| '__| | | | | '_ ` _ \| | | |  _ \ 
  / ___ \ |_| |  | | |_| | | | | | | |_| | |_) |
 /_/   \_\__|_|  |_|\__,_|_| |_| |_|____/|____/ 
""")

print("test")






