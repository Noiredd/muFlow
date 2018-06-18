from baseTasks import BaseProcessor, BaseParallel

class TestTask(BaseProcessor):
  name = 'test'
  outputs = ['item']
  def action(self):
    return 2

class TestTask2(BaseProcessor):
  name = 'add'
  params  = [('const', int)]
  inputs  = ['item']
  outputs = ['item']
  def setup(self):
    self.ready = True
  def action(self, item):
    return item + self.const

class TestTask3(BaseProcessor):
  name = 'dup'
  inputs = ['item']
  outputs = ['item', 'thing']
  def action(self, x):
    return x, x

class TestTask4(BaseProcessor):
  name = 'list'
  params = [('length', int)]
  outputs = ['items']
  def action(self):
    return [i for i in range(self.length)]

class ParallelTask1(BaseParallel):
  name = 'inc_each'
  inputs = ['items']
  outputs = ['items']
  def action(self, item):
    return item + 1

class ParallelTask2(BaseParallel):
  name = 'print'
  inputs = ['item']
  def action(self, i):
    print(i)
    return None

class ParallelTask3(BaseParallel):
  name = 'prints'
  inputs = ['a','b']
  def action(self, i, j):
    print(i+j)
    return None
