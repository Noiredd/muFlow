from baseTasks import BaseProcessor

class TestTaskSource(BaseProcessor):
  name = 'src'
  info = 'Outputs a single int given by param'
  params = [('value', int)]
  outputs = ['item']
  def action(self):
    return self.value

class TestTaskAdd(BaseProcessor):
  name = 'add'
  info = 'Adds a constant given by param to its input'
  params  = [('const', int)]
  inputs  = ['item']
  outputs = ['item']
  def setup(self):
    self.ready = True
  def action(self, item):
    return item + self.const

class TestTaskDuplicate(BaseProcessor):
  name = 'dup'
  info = 'Outputs two copies of its input'
  inputs = ['item']
  outputs = ['item', 'thing']
  def action(self, x):
    return x, x

class TestTaskCreateList(BaseProcessor):
  name = 'lst'
  info = 'Outputs a list of given length, containing numbers 0..(length-1)'
  params = [('count', int)]
  outputs = ['item']
  def action(self):
    return [i for i in range(self.count)]

class TestDebugGetter(BaseProcessor):
  name = 'get'
  info = 'Idle task that can be used to force a MicroFlow to gather a specified item'
  inputs = ['item']
