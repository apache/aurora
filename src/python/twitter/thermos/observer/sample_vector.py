class SampleVector(object):
  MAX_SAMPLES = 1000

  def __init__(self, name):
    self._samples = []
    self._name    = name

  def name(self):
    return self._name

  def max_sample(self):
    return max(s[1] for s in self._samples) if len(self._samples) > 0 else 0

  def num_samples(self):
    return len(self._samples)

  def last_sample(self, N=1):
    if len(self._samples) >= N:
      return self._samples[-N:]
    else:
      return None

  def running_at(self, at_time):
    return (self._samples[0][0] <= at_time and at_time <= self._samples[-1][0])

  def add(self, time, rate):
    self._samples.append( (time, rate) )
    if len(self._samples) > SampleVector.MAX_SAMPLES:
      self._samples = self._samples[-SampleVector.MAX_SAMPLES:]

  def since(self, timestamp):
    return filter(lambda sample: sample[0] >= timestamp, self._samples)

  def __str__(self):
    return 'samples: %s' % ' '.join(['%.2f' % s[1] for s in self._samples])

  def __add__(self, other):
    x = set([s[0] for s in self._samples] + [s[0] for s in other._samples])
    x = list(x)
    x.sort()

    cursor_1, cursor_2 = 0, 0
    len_self, len_other = len(self._samples), len(other._samples)
    output = []

    for k in range(len(x)):
      sample = 0
      while cursor_1 < len_self and x[k] > self._samples[cursor_1][0]: cursor_1 += 1
      while cursor_2 < len_other and x[k] > other._samples[cursor_2][0]: cursor_2 += 1

      if cursor_1 < len_self and x[k] == self._samples[cursor_1][0]:
        sample += self._samples[cursor_1][1]
      if cursor_2 < len_other and x[k] == other._samples[cursor_2][0]:
        sample += other._samples[cursor_2][1]

      output.append( (x[k], sample) )

    new_a = SampleVector(self.name())
    new_a._samples = output
    return new_a
