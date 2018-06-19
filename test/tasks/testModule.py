from baseTasks import BaseProcessor

class TestTaskSource(BaseProcessor):
  name = 'src'
  params = [('value', int)]
  outputs = ['item']
  def action(self):
    return self.value

class TestTaskAdd(BaseProcessor):
  name = 'add'
  params  = [('const', int)]
  inputs  = ['item']
  outputs = ['item']
  def setup(self):
    self.ready = True
  def action(self, item):
    return item + self.const

class TestTaskDuplicate(BaseProcessor):
  name = 'dup'
  inputs = ['item']
  outputs = ['item', 'thing']
  def action(self, x):
    return x, x

class TestTaskCreateList(BaseProcessor):
  name = 'lst'
  params = [('count', int)]
  outputs = ['item']
  def action(self):
    return [i for i in range(self.count)]

class TestDebugGetter(BaseProcessor):
  name = 'get'
  inputs = ['item']
