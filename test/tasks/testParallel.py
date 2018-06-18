from baseTasks import BaseParallel

class TestTaskCreateList(BaseParallel):
  name = 'list'
  params = [('count', int)]
  outputs = ['items']
  def action(self):
    return [i for i in range(self.count)]

class TestTaskIncrementParallel(BaseParallel):
  name = 'incr'
  inputs = ['items']
  outputs = ['items']
  def action(self, item):
    return item + 1
