from twisted.internet import defer
from twisted.protocols import basic


class CalculatorClient(basic.LineReceiver):

    def __init__(self):
        self.results = []

    def lineReceived(self, line):
        d = self.results.pop(0)
        d.callback(line)

    def send(self, calculation_id):
        d = defer.Deferred()
        self.results.append(d)
        self.sendLine(calculation_id)
        return d
