from baseTasks import BaseProcessor, BaseReducer

class SumReduce(BaseReducer):
  name = 'sum'
  info = 'Calculates sum of the elements, assuming they support a \'+\' operator.'
  inputs = ['item']
  outputs = ['item']
  def reduction(self, a, b):
    return a + b
