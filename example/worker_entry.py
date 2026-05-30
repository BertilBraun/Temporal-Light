import logging
import os

from temporal_light import Worker

from flows import cancel_order, charge_payment, order_flow, risk_check_flow, send_receipt

logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    Worker(
        workflow_functions=[order_flow, risk_check_flow],
        activity_functions=[charge_payment, send_receipt, cancel_order],
        database_url=os.environ['DATABASE_URL'],
        worker_concurrency=int(os.environ.get('WORKER_CONCURRENCY', '4')),
    ).run()
