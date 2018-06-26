import argparse
import sys

from assembler import Assembler
from baseTasks import muException

if __name__ == "__main__":
  p = argparse.ArgumentParser(description='muFlow, the parallel processing engine')
  p.add_argument('script', nargs='?', default=None, help='Script to run')
  p.add_argument('--num-processes', '-n', type=int, default=0,
      help='Number of processes to spawn (default is CPU count)')
  p.add_argument('--debug', action='store_true',
      help='Debug mode: each parallel task processes only the first item in a single process')
  p.add_argument('--info', type=str, nargs='?', const='', default=None,
      help='Print list of available tasks, OR, if followed by a name of task, details of this task.')
  p.add_argument('--no-vt', action='store_true', help='Do not use VT100 escape codes')
  p.add_argument('--no-log', action='store_true', help='Do not print logs and time measurements')

  asm = Assembler()

  args = p.parse_args()
  if args.no_vt:
    asm.preventVT100()
  if args.no_log:
    asm.preventLogging()
  if args.info is not None:
    asm.printInfo(args.info if args.info != '' else None)
    exit()
  if args.script is None:
    print("You are supposed to pass a path to the script file as the first positional argument.")
    exit()

  with open(args.script, 'r') as script_file:
    script = [line.strip('\n') for line in script_file.readlines()]
  try:
    flow = asm.assembleFromText(lines=script,
                                num_proc=args.num_processes,
                                debug=args.debug
    )
  except muException as e:
    e.die()
  
  flow.execute()
