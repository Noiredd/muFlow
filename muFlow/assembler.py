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
          self.tasks[obj.name] = obj
