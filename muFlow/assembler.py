import importlib
import inspect
import os
import sys

import baseTasks

class Assembler(object):
  taskFolder = 'tasks/'
  tasks_serial = {}
  tasks_parallel = {}
  
  def __init__(self, altPath=None):
    self.__importTasks(self.taskFolder if altPath is None else altPath)
  
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
            not obj==baseTasks.BaseProcessor and not obj==baseTasks.BaseParallel):
          try:
            obj.validateParams()
          except baseTasks.BadParamException as e:
            print(e.message, '- skipping import')
          else:
            #any task that reached this point is good to go, regardless of what kind it was
            #but now it matters whether it's parallel or serial
            if issubclass(obj, baseTasks.BaseParallel):
              self.tasks_parallel[obj.name] = obj
            else:
              self.tasks_serial[obj.name] = obj
  
  def constructTask(self, text):
    #parses a given text (line) and attempts to construct a task from it
    args = text.split()
    task_name = args[0]
    task_args = args[1:]
    if task_name in self.tasks_serial.keys():
      return True, self.tasks_serial[task_name](*task_args)
    elif task_name in self.tasks_parallel.keys():
      return False, self.tasks_parallel[task_name](*task_args)
    else:
      raise ConstructException(task_name, 'no such task')
  
  def assembleFromText(self, lines):
    flow = MacroFlow()
    for n, line in enumerate(lines):
      #more flexibility in IO override text
      if '(' in line:
        if line.count('(') > 1 or line.count(')') != 1:
          raise ConstructException('line {}: '.format(n), 'unbalanced brackets')
        despace = lambda x: ''.join(x.split())
        pre, btwn = line.split('(')
        btwn, post = btwn.split(')')
        line = despace(pre) + ' (' + despace(btwn) + ') ' + post
      isSerial, task = self.constructTask(line)
      if isSerial:
        flow.appendSerial(task)
      else:
        flow.appendParallel(task)
    flow.completeParallel() #ensure the parallel tasks are assembled
    return flow

class MacroFlow(object):
  def __init__(self):
    self.tasks = [] #tasks are executed in order
    self.scope = {} #place where the intermediate results are contained
    self.parallel = None  #MicroFlow object during construction
  
  def appendSerial(self, task):
    #append a serial task to the execution list
    #if a parallel task was added before, its MicroFlow has to be completed
    if self.parallel is not None:
      self.completeParallel()
    #first check whether the scope contains items that this task would request
    requests = task.getInputs()
    for request in requests:
      if request not in self.scope.keys():
        raise ConstructException(task.name, 'requests "' + request + '" which is not in scope yet')
    #if this has passed, update the scope and append the task
    for output in task.getOutputs():
      self.scope[output] = None
    self.tasks.append(task)
  
  def appendParallel(self, task):
    if self.parallel is None:
      #we're only starting to construct a parallelized task set
      self.parallel = MicroFlow(self.scope) #let the sub-flow know what items do we already have
    self.parallel.append(task)
  
  def completeParallel(self):
    #when the series of parallel tasks ends, append it to the task list
    if self.parallel is not None:
      task = self.parallel
      self.parallel = None
      self.appendSerial(task) #treat it as a serial task
  
  def execute(self):
    #start with setting up each task
    for task in self.tasks:
      task.setup()
    #execute the task list in order
    for task in self.tasks:
      #query each task for its required inputs and retrieve their values from the scope
      inputs = [self.scope[i] for i in task.getInputs()]
      #feed them to the task and run it
      results = task.action(*inputs) if inputs is not [] else task.action()
      #pack the output back to the scope
      #mind that if there's a single output, it will be passed as is,
      #but multiple outputs will be packed into a tuple
      outputs = task.getOutputs()
      if len(outputs) > 1:
        for result, key in zip(results, outputs):
          self.scope[key] = result
      elif len(outputs) == 1:
        self.scope[outputs[0]] = results
      else:
        pass
    return self.scope

class MicroFlow(object):
  def __init__(self, macro_scope):
    self.tasks = []
    self.macro_scope = macro_scope
    self.micro_scope = {}   #just for building phase
    self.map_requests = []  #those items aren't produced by either of the local tasks
  
  def append(self, task):
    requests = task.getInputs()
    for request in requests:
      if request not in self.micro_scope.keys():
        #if the task requests an item that is not in the local scope, it might be in the macro
        if request in self.macro_scope.keys():
          self.map_requests.append(request)
        else:
          raise ConstructException(task.name, 'requests "' + request + '" which is not in scope yet')
    for output in task.getOutputs():
      self.micro_scope[output] = None
    self.tasks.append(task)
  
  def getInputs(self):
    return self.map_requests
  
  def getOutputs(self):
    return []

  #quack like a BaseProcessor
  def setup(self, **kwargs):
    #this would instantiate a thread/process pool so that action() just starts it
    pass

  def action(self, *args):
    #iterate over all args in parallel in such a way that every set of values is a dict
    #this will be split onto multiple threads/processes
    for this_scope in [dict(zip(self.map_requests, pack)) for pack in zip(*args)]:
      #local, non-persistent scope
      scope = {}
      #iterate over the sequence of tasks
      for task in self.tasks:
        #collect inputs, either from the global or local scope
        inputs = [this_scope[req] if req in self.map_requests else scope[req] for req in task.getInputs()]
        results = task.action(*inputs) if inputs is not [] else task.action()
        #pack outputs back but into the micro scope
        outputs = task.getOutputs()
        if len(outputs) > 1:
          for result, key in zip(results, outputs):
            scope[key] = result
        elif len(outputs) == 1:
          scope[outputs[0]] = results
        else:
          pass


class ConstructException(baseTasks.muException):
  def __init__(self, taskname, text):
    self.message = text + ' (' + taskname + ')'
    super(ConstructException, self).__init__(self.message)
