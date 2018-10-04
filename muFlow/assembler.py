import importlib
import inspect
import multiprocessing as mp
import os
import sys
from copy import copy

import baseTasks
import muparse
import progress
from errors import *

class Assembler(object):
  taskFolder = 'tasks/'
  tasks_serial = {}
  tasks_reducer = {}
  tasks_parallel = {}
  
  def __init__(self, altPath=None):
    if altPath is None:
      thisDir = os.path.dirname(os.path.abspath(__file__))
      rootDir, _ = os.path.split(thisDir)
      self.taskFolder = os.path.join(rootDir, 'tasks')
    else:
      self.taskFolder = altPath
    self.parser = muparse.Parser()
    self.__importTasks(self.taskFolder)

  def __importTasks(self, path):
    if not path.endswith('/'): path += '/'
    #scan the folder for modules
    module_names = []
    for module in os.listdir(path):
      if os.path.isfile(path + module) and module.endswith('.py'):
        module_names.append( module.split('.')[0] )
    #import each and extract only the classes that derive from BaseProcessor
    sys.path.append(path)
    for module in module_names:
      tmod = importlib.import_module(module)
      for name, obj in inspect.getmembers(tmod, inspect.isclass):
        if (issubclass(obj, baseTasks.BaseProcessor) and
            not obj==baseTasks.BaseProcessor and
            not obj==baseTasks.BaseParallel and
            not obj==baseTasks.BaseReducer):
          try:
            if not obj.isValid:
              obj.validateParams()
          except baseTasks.BadParamException as e:
            print(e.message + ' - skipping import')
          else:
            #add a string attribute identifying which module did the task come from
            obj.module = module
            #any task that reached this point is good to go, regardless of what kind it was
            #but now it matters whether it's parallel or serial
            if issubclass(obj, baseTasks.BaseParallel):
              self.tasks_parallel[obj.name] = obj
            elif issubclass(obj, baseTasks.BaseReducer):
              self.tasks_reducer[obj.name] = obj
            else:
              self.tasks_serial[obj.name] = obj

  def printTaskDetails(self, task):
    s = lambda x: str(x).split("'")[1]
    print('\t{}'.format(task.info))
    print('\tParams:  {}'.format(len(task.params)))
    if len(task.params) > 0:
      for param in task.params:
        default = lambda x: '' if len(x) < 3 else ', default = {}'.format(x[2])
        print('\t\t{:10}({}){}'.format(param[0], s(param[1]), default(param)))
    print('\tInputs:  {}, default: {}'.format(len(task.inputs), ','.join(task.inputs)))
    print('\tOutputs: {}, default: {}'.format(len(task.outputs), ','.join(task.outputs)))

  def printInfo(self, task_=None):
    if task_ is not None:
      if task_ in self.tasks_serial:
        task = self.tasks_serial[task_]
        print('{} [{}.py] (serial)'.format(task.name, task.module))
        self.printTaskDetails(task)
      elif task_ in self.tasks_reducer:
        task = self.tasks_reducer[task_]
        print('{} [{}.py] (reducer)'.format(task.name, task.module))
        self.printTaskDetails(task)
      elif task_ in self.tasks_parallel:
        task = self.tasks_parallel[task_]
        print('{} [{}.py] (parallel)'.format(task.name, task.module))
        self.printTaskDetails(task)
      else:
        print('There is no task "{}".'.format(task_))
    else:
      def __info(t, m):
        print('\t{:{pad}}  [{mod}.py]'.format(t, pad=maxlen,  mod=m))
      sTasks = sorted(self.tasks_serial.keys())
      rTasks = sorted(self.tasks_reducer.keys())
      pTasks = sorted(self.tasks_parallel.keys())
      maxlen = max([len(task) for task in sTasks+rTasks+pTasks])
      print('List of available serial tasks:')
      for task in sTasks:
        if task == '': continue
        __info(task, self.tasks_serial[task].module)
      print('List of available reducer tasks:')
      for task in rTasks:
        if task == '': continue
        __info(task, self.tasks_reducer[task].module)
      print('List of available parallel tasks:')
      for task in pTasks:
        if task == '': continue
        __info(task, self.tasks_parallel[task].module)

  def preventVT100(self):
    progress.useVT100(False)

  def preventLogging(self):
    progress.usePrint(False)

  def assembleFromText(self, lines, num_proc=0, debug=False):
    #parses the given text line by line, constructing tasks from them
    flow = MacroFlow(num_proc=num_proc, debug=debug)
    for n, line in enumerate(lines):
      #parse the line into a dict of construction information
      #(task name, input/output arguments, parameters)
      taskData = self.parser.parseText(line)
      taskName = taskData['name']
      #allow empty (or commented) lines
      if taskName == '':
        continue
      #try to find the task in the task lists, starting from parallel
      if taskName in self.tasks_parallel.keys():
        taskClass = self.tasks_parallel[taskName]
        taskObject = taskClass(**taskData)
        flow.appendParallel(taskObject)
      elif taskName in self.tasks_serial.keys():
        taskClass = self.tasks_serial[taskName]
        taskObject = taskClass(**taskData)
        flow.appendSerial(taskObject)
      elif taskName in self.tasks_reducer.keys():
        taskClass = self.tasks_reducer[taskName]
        taskObject = taskClass(**taskData)
        flow.appendReducer(taskObject)
      else:
        raise ConstructException('line {}: '.format(n), 'no such task!')
    flow.completeParallel() #ensure the parallel tasks are assembled
    return flow

class MacroFlow(object):
  def __init__(self, num_proc=0, debug=False):
    self.debug = debug
    self.tasks = [] #tasks are executed in order
    self.scope = {} #place where the intermediate results are contained
    self.micros = []      #potential outputs of MicroFlows; list of tuples
    self.parallel = None  #MicroFlow object during construction
    self.num_proc = num_proc
    self.deferred = []    #serial reducers to be added after the MicroFlow
  
  def appendSerial(self, task, isMicro=False):
    #append a serial task to the execution list
    #if a parallel task was added before, its MicroFlow has to be completed
    if self.parallel is not None:
      self.completeParallel()
    #check whether the task's input requirements can be satisfied
    requests = task.getInputs()
    for request in requests:
      found = False
      #see if any of the parallel tasks can produce that item
      for micro in self.micros:
        if request in micro[1]:
          microFlowTaskID = micro[0]
          #kindly ask that task to output it
          self.tasks[microFlowTaskID].gather(request)
          #and assume it will obey
          self.scope[request] = None
          #no need to look through the macro scope
          found = True
          break
      if not found and request not in self.scope.keys():
        raise ConstructException(task.name, 'requests "' + request + '" which is not in scope yet')
    #if it *is* a parallel task being added, mind that it doesn't output anything by default
    #instead, ask it what could it possibly output and keep track of that
    #if a serial task comes later and requests something from that potential scope,
    #the parallel task will be instructed to actually produce that output
    if isMicro:
      self.micros.append( (len(self.tasks), task.micro_scope) )
    else:
      for output in task.getOutputs():
        self.scope[output] = None
    self.tasks.append(task)
  
  def appendParallel(self, task, isReducer=False):
    if self.parallel is None:
      #we're only starting to construct a parallelized task set
      self.parallel = MicroFlow(macro_scope=self.scope,
                                num_proc=self.num_proc,
                                debug=self.debug
      )
    self.parallel.append(task, isReducer)
  
  def appendReducer(self, task):
    #Reducer is added as both a parallel and a serial task. A parallel task is
    #added to the MicroFlow to reduce the sub-lists into sub-results, which then
    #get reduced to the final result using a serial task that will be appended
    #right after that MicroFlow. The serial variant has to be modified though
    #to iterate over its input.
    #Create a serial copy of the original
    serial = copy(task)
    #Add the parallel variant, but be sure to change its output so that the
    #serial variant receives it!
    task.outputs = task.inputs
    self.appendParallel(task, isReducer=True)
    #Prepare the serial one but efer its addition until MicroFlow is completed
    serial.action = serial.action_final
    self.deferred.append(serial)
  
  def completeParallel(self):
    #when the series of parallel tasks ends, append it to the task list
    if self.parallel is not None:
      task = self.parallel
      self.parallel = None
      self.appendSerial(task, isMicro=True) #treat it *almost* as a serial task
      #add the deferred reducers, if any
      for serial in self.deferred:
        self.appendSerial(serial)
      self.deferred = []
  
  def execute(self):
    #measure time and print reports using external object
    reporter = progress.SerialReporter()
    #start with setting up each task
    reporter.start('Setting up...')
    try:
      for task in self.tasks:
        task.setup()
    except baseTasks.muException as e:
      e.die()
    reporter.stop()
    #execute the task list in order
    for task in self.tasks:
      reporter.start('Task: ' + task.name)
      #query each task for its required inputs and retrieve their values from the scope
      inputs = [self.scope[i] for i in task.getInputs()]
      #feed them to the task and run it
      results = task.action(*inputs)
      #pack the output back to the scope
      #mind that if a task returns multiple items, they are automatically packed into
      #a tuple, while a single item is packed as is and should not be iterated over
      outputs = task.getOutputs()
      for result, key in zip(results if len(outputs) > 1 else (results,), outputs):
        self.scope[key] = result
      reporter.stop()
    reporter.total('Done!')
    return self.scope

class MicroFlow(object):
  def __init__(self, macro_scope, num_proc=0, debug=False):
    self.name  = 'MicroFlow'
    self.debug = debug
    self.tasks = []
    self.reducers = []
    self.gathered = []
    self.macro_scope = macro_scope
    self.micro_scope = set()  #just for building phase
    self.map_requests = []  #those items aren't produced by either of the local tasks
    self.num_proc = num_proc if num_proc > 0 else mp.cpu_count()
    if self.debug:
      self.num_proc = 1

  def __makeName(self):
    #builds a name string from tasks on the list - best call that at setup
    #shortens the list longer than 5 tasks
    if len(self.tasks) > 5:
      _name = self.tasks[0].name + ', ' + self.tasks[1].name + ', ... , ' + self.tasks[-1].name
    else:
      _name = ', '.join([task.name for task in self.tasks])
    self.name += ' (' + _name + ')'
  
  def append(self, task, isReducer=False):
    requests = task.getInputs()
    for request in requests:
      if request not in self.micro_scope:
        #if the task requests an item that is not in the local scope, it might be in the macro
        if request in self.macro_scope.keys():
          self.map_requests.append(request)
        else:
          raise ConstructException(task.name, 'requests "' + request + '" which is not in scope yet')
    for output in task.getOutputs():
      self.micro_scope.add(output)
    self.tasks.append(task)
    #if the task is also a reducer, we need to know about it
    if isReducer:
      self.reducers.append(task)
      for output in task.getOutputs():
        self.gather(output)
  
  def getInputs(self):
    return self.map_requests
  
  def getOutputs(self):
    return self.gathered
  
  def gather(self, item):
    #set to actually produce an item by given name
    if item not in self.micro_scope:
      raise ConstructException('MicroFlow', 'item "' + item + '" is not in the local scope!')
    #ignore if we're already gathering this item
    if item not in self.gathered:
      self.gathered.append(item)

  #quack like a BaseProcessor
  def setup(self, **kwargs):
    #task list is complete so we can construct the name string
    self.__makeName()
    self.reporter = progress.ParallelReporter('Task: ' + self.name)
    self.pipes = []
    self.pool = []
    for i in range(self.num_proc):
      a, b = mp.Pipe(True)  #duplex pipe - we send data and receive results
      self.pipes.append(a)
      self.pool.append(
        mp.Process(target=self.sequence, args=(b,) if i > 0 else (b,self.reporter))
      )
    #setup the parallel tasks
    for task in self.tasks:
      task.setup()

  def action(self, *args):
    #args are lists (assumed of equal size) which need to be cut into slices
    #for each of the processes and sent to them as soon as they are started
    batch_size = (len(args[0]) // self.num_proc + 1) if not self.debug else 1
    for i in range(self.num_proc):
      n_beg = i * batch_size
      n_end = n_beg + batch_size
      batch = [arg[n_beg:n_end] for arg in args]
      self.pool[i].start()
      self.pipes[i].send(batch)
    #collect the outputs and wait until the processes are done
    outputs = [pipe.recv() for pipe in self.pipes]
    for process in self.pool:
      process.join()
    #merge the results
    results = {item: [] for item in self.gathered}
    for item in self.gathered:
      for output in outputs:
        results[item] += output[item]
    #output
    if len(self.gathered) > 1:
      return [results[key] for key in self.gathered]
    elif len(self.gathered) == 1:
      return results[self.gathered[0]]
    else:
      return None
  
  def sequence(self, pipe, progress=None):
    #this function is executed by each process separately
    #start by receiving data (list of args, each being a list)
    input_data = pipe.recv()
    #prepare to gather the outputs
    collect = {item: [] for item in self.gathered}
    #iterate over lists that make up the input data
    for data in zip(*input_data):
      #construct a local scope
      scope = dict(zip(self.map_requests, data))
      #iterate over the sequence of tasks
      for task in self.tasks:
        inputs = [scope[req] for req in task.getInputs()]
        results = task.action(*inputs)
        #pack outputs back but into the micro scope
        outputs = task.getOutputs()
        for result, key in zip(results if len(outputs) > 1 else (results,), outputs):
          scope[key] = result
      #if anything from the local scope was marked as gathered - do so
      for item in self.gathered:
        collect[item].append( scope[item] )
      #report progress, if so requested (disabled by default)
      if progress is not None: progress(len(input_data[0]))
    #collect the outputs of any reduction tasks
    for task in self.reducers:
      for item in task.getOutputs():
        collect[item] = task.output()
    #send the outputs over the pipe
    pipe.send(collect)
