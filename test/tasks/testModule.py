from baseTasks import BaseProcessor

class TestTask(BaseProcessor):
  name = 'test'
  def action(self, item):
    return 21
