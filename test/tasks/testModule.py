from baseTasks import BaseProcessor

class TestTask(BaseProcessor):
  name = 'test'
  def action(self, item):
    return item * 2

class TestTask2(BaseProcessor):
  name = 'add'
  def action(self, item):
    return item + 1
