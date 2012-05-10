from twisted.protocols import basic
from twisted.internet import protocol
from lib.calculator import Calculator


class RemoteCalculatorProtocol(basic.LineReceiver):

    def __init__(self):
        self.calc = Calculator()

    def lineReceived(self, calculation_id):
        self.calc.run(calculation_id.strip())
        self.sendLine('1')


class RemoteCalculatorFactory(protocol.Factory):
    protocol = RemoteCalculatorProtocol


def main():
    from twisted.internet import reactor
    from twisted.python import log
    import sys
    log.startLogging(sys.stdout)
    reactor.listenTCP(0, RemoteCalculatorFactory())
    reactor.run()


if __name__ == '__main__':
    main()
