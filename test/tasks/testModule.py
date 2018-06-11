from baseTasks import BaseProcessor

class TestTask(BaseProcessor):
  name = 'test'
  def result(self, item):
    return [item]
  def action(self):
    return self.result(2)

class TestTask2(BaseProcessor):
  name = 'add'
  def result(self, item):
    return [item]
  def action(self, item):
    return self.result(item + 1)
