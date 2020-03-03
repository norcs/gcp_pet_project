import transaction_sim
import transaction_sub

import multiprocessing


def simulate_transactions():
    print('Process 1 started...')
    transaction_sim.run()


def transaction_subscriber():
    print('Process 2 started...')
    transaction_sub.run()


if __name__ == '__main__':
    p1 = multiprocessing.Process(target=simulate_transactions)
    p2 = multiprocessing.Process(target=transaction_subscriber)
    p1.start()
    p2.start()

