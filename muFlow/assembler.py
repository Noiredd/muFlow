import importlib
import inspect
import os
import sys

import baseTasks

class Assembler(object):
  taskFolder = 'tasks/'
  
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
    self.tasks = {}
    sys.path.append(path)
    for module in module_names:
      tmod = importlib.import_module(module)
      for name, obj in inspect.getmembers(tmod, inspect.isclass):
        if issubclass(obj, baseTasks.BaseProcessor) and not obj==baseTasks.BaseProcessor:
          try:
            obj.validateParams()
          except baseTasks.BadParamException as e:
            print(e.message, '- skipping import')
          else:
            self.tasks[obj.name] = obj
  
  def constructTask(self, text):
    #parses a given text (line) and attempts to construct a task from it
    args = text.split()
    task_name = args[0]
    task_args = args[1:]
    if task_name not in self.tasks.keys():
      raise ConstructException(task_name, 'no such task')
    return self.tasks[task_name](*task_args)
  
  def assembleFromText(self, lines):
    flow = FlowObject()
    for line in lines:
      task = self.constructTask(line)
      flow.append(task)
    return flow

class FlowObject(object):
  def __init__(self):
    self.tasks = [] #tasks are executed in order
    self.scope = {} #place where the intermediate results are contained
  
  def append(self, task):
    #append the task to the execution list
    #first check whether the scope contains items that this task would request
    requests = task.getInputs()
    for request in requests:
      if request not in self.scope.keys():
        raise ConstructException(task.name, 'requests "' + request + '" which is not in scope yet')
    #if this has passed, update the scope and append the task
    for output in task.getOutputs():
      self.scope[output] = None
    self.tasks.append(task)
  
  def execute(self):
    #execute the task list in order
    for task in self.tasks:
      #query each task for its required inputs and retrieve their values from the scope
      inputs = [self.scope[i] for i in task.getInputs()]
      #feed them to the task and run it
      results = task.action(*inputs) if inputs is not [] else task.action()
      #pack the output back to the scope
      for result, key in zip(results, task.getOutputs()):
        self.scope[key] = result
    return self.scope

class ConstructException(baseTasks.muException):
  def __init__(self, taskname, text):
    self.message = text + ' (' + taskname + ')'
    super(ConstructException, self).__init__(self.message)
