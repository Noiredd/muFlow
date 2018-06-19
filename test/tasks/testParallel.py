from baseTasks import BaseParallel

class TestTaskIncrementParallel(BaseParallel):
  name = 'incr'
  params = [('const', int)]
  inputs = ['items']
  outputs = ['items']
  def action(self, item):
    return item + self.const

class TestTaskMultiplyParallel(BaseParallel):
  name = 'vmul'
  inputs = ['x', 'y']
  outputs = ['items']
  def action(self, x, y):
    return x * y
