from baseTasks import BaseProcessor

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
  def action(self, item):
    return item + self.const

class TestTask3(BaseProcessor):
  name = 'dup'
  inputs = ['item']
  outputs = ['item', 'thing']
  def action(self, x):
    return x, x
