import logging
import time
from datetime import datetime
from logging import Logger

from pushbullet import Pushbullet
from slacker import Slacker
from trading_bots.bots import Bot
from trading_bots.conf import settings
from trading_bots.contrib.clients import Market, Side
from trading_bots.contrib.clients import buda
from trading_bots.utils import truncate_to


class Notifier:

    def __init__(self, tag: str, logger: Logger=None):
        self.config = settings.slack
        self.tag = tag
        self.slack = Slacker(settings.credentials['Slack']['key'])
        self.pb = Pushbullet(settings.credentials['Pushbullet']['key'])
        self.log = logger or logging.getLogger('Notifier')

    def notify(self, message: str):
        t = time.strftime('%Y-%m-%d %H:%M:%S %z', time.localtime())
        tag = f'[{t} {self.tag}]'
        try:
            self.slack.chat.post_message(
                channel=self.config['channel'],
                username=self.config['username'],
                text=f'{tag} {message}',
                parse='full'
            )
            self.pb.push_note(title=tag, body=message)
        except Exception:
            self.log.warning(f'Notify failed: {tag} {message}')


class AnyToAny(Bot):
    label = 'AnyToAny'

    def _setup(self, config):
        # Get configs
        self.from_currency = config['from']['currency']
        self.from_address = config['from']['address']
        self.to_currency = config['to']['currency']
        self.to_withdraw = config['to']['withdraw']
        self.to_address = config['to']['address']
        # Set market
        self.market = self._get_market(self.from_currency, self.to_currency)
        # Set side
        self.side = Side.SELL if self.market.base == self.from_currency else Side.BUY
        # Set Buda trading client
        host = settings.urls['buda']
        self.buda = buda.BudaTrading(
            self.market, dry_run=self.dry_run, timeout=self.timeout, logger=self.log, store=self.store, host=host)
        # Get deposits from store
        self.deposits = self.store.get(self.from_currency + '_deposits') or {}
        # Set start date
        self.start_date = self.get_start_date()
        # Set notifier client
        self.notifier = Notifier(tag=self.label, logger=self.log)

    def _algorithm(self):
        # Get new deposits
        self.log.info(f'Checking for new {self.from_currency} deposits')
        self.update_deposits()
        # Convert pending amounts
        self.log.info('Converting pending amounts')
        self.process_conversions()
        # Get available balances
        self.log.info('Processing pending withdrawals')
        self.process_withdrawals()

    def _abort(self):
        pass

    def store_deposits(self):
        self.store.store(self.from_currency + '_deposits', self.deposits)

    def _add_deposit(self, idx, state, original, pending):
        self.deposits[idx] = {
            'state': state,
            'amounts': {'original_amount': original,
                        'converted_amount': 0,
                        'converted_value': 0},
            'orders': [],
            'pending_withdrawal': pending,
        }

    def get_start_date(self):
        start = self.store.get('start')
        if not start:
            start = time.time()
            self.store.set('start', start)
        return datetime.utcfromtimestamp(start)

    def update_deposits(self):
        # Set wallet from relevant currency according to side
        from_wallet = self.buda.wallets.quote if self.side == Side.BUY else self.buda.wallets.base
        # Get and filter deposits
        new_deposits = from_wallet.get_deposits()
        if self.from_address != 'Any':
            new_deposits = [d for d in new_deposits if d.data.address == self.from_address]
        new_deposits = [d for d in new_deposits if d.created_at >= self.start_date]
        # Update states on existing keys and add new keys with base structure
        for deposit in new_deposits:
            idx = str(deposit.id)
            if idx in self.deposits.keys():
                if deposit.state != self.deposits[idx]['state']:
                    self.deposits[idx]['state'] = deposit.state
                    self.notifier.notify(f'Deposit {idx} state changed to {deposit.state}')
            else:
                self._add_deposit(idx, deposit.state, deposit.amount.amount, self.to_withdraw)
                self.notifier.notify(f'New deposit detected: id: {idx} | currency: {deposit.amount.currency} | '
                                     f'amount: {deposit.amount.amount} | state: {deposit.state}')
            self.store_deposits()

    def process_conversions(self):
        # Get deposits
        for deposit in self.deposits.values():
            # Calculate remaining amount to convert
            original_amount = deposit['amounts']['original_amount']
            converted_amount = deposit['amounts']['converted_amount']
            converted_value = deposit['amounts']['converted_value']
            remaining = original_amount - converted_amount
            if deposit['state'] == 'confirmed' and remaining > 0:
                if self.side == Side.BUY:  # Change amount to base currency for order creation purposes
                    quotation = self.buda.client.quotation_market(
                        market_id=self.buda.market_id, quotation_type='bid_given_spent_quote', amount=remaining)
                    remaining = quotation.order_amount.amount
                remaining = truncate_to(remaining, self.market.base)
                # Convert remaining amount using market order
                order = self.buda.place_market_order(self.side, remaining)
                self.notifier.notify(f'{self.side.value}ing {remaining} {self.market.base} at market rate')
                # Wait for traded state to set updated values
                if order:
                    self.log.info(f'{self.side} market order placed, waiting for traded state')
                    while order.state != 'traded':
                        order = self.buda.client.order_details(order.id)
                        time.sleep(1)
                    self.log.info(f'{self.side} order traded, updating store values')
                    # Update amounts
                    if self.side == Side.BUY and order.state == 'traded':
                        converted_amount += order.total_exchanged.amount
                        converted_value += order.traded_amount.amount
                    else:
                        converted_amount += order.traded_amount.amount
                        converted_value += order.total_exchanged.amount
                    converted_value -= order.paid_fee.amount  # Fee deducted so it wont interfere with withdrawal
                    deposit['orders'].append(order.id)  # Save related orders for debugging
                    self.notifier.notify(f'Success!, converted value: {converted_value} {self.to_currency}')
                # Save new values
                deposit['amounts']['converted_amount'] = converted_amount
                deposit['amounts']['converted_value'] = converted_value
                self.store_deposits()

    def process_withdrawals(self):
        # Set wallet from relevant currency according to side
        to_wallet = self.buda.wallets.base if self.side == Side.BUY else self.buda.wallets.quote
        for deposit in self.deposits.values():
            # Filter deposits already converted and pending withdrawal
            deposit_confirmed = deposit['state'] == 'confirmed'
            has_pending_withdrawal = deposit['pending_withdrawal']
            converted_all = deposit['amounts']['original_amount'] == deposit['amounts']['converted_amount']
            if deposit_confirmed and has_pending_withdrawal and converted_all:
                withdrawal_amount = truncate_to(deposit['amounts']['converted_value'], self.to_currency)
                available = to_wallet.get_available()
                if withdrawal_amount <= available:  # We cannot withdraw more than available balance
                    self.notifier.notify(f'Withdrawing {withdrawal_amount} {self.to_currency}')
                    withdrawal = to_wallet.request_withdrawal(withdrawal_amount, self.to_address, subtract_fee=True)
                    if withdrawal.state == 'pending_preparation':  # Check state to set and store updated values
                        self.log.info(f'{self.to_currency} withdrawal request received, updating store values')
                        deposit['pending_withdrawal'] = False
                        self.store.store(self.from_currency + '_deposits', self.deposits)
                        self.notifier.notify(f'Success!, withdrawal id: {withdrawal.id} {self.to_currency}')
                    else:
                        msg = 'Withdrawal failed'
                        self.log.warning(msg)
                        self.notifier.notify(f'{msg}, :shame:')
                else:
                    msg = f'Available balance not enough for withdrawal amount {withdrawal_amount} {self.to_currency}'
                    self.log.warning(msg)
                    self.notifier.notify(msg)

    def _get_market(self, from_currency, to_currency):
        public_client = buda.BudaPublic()
        buda_markets = public_client.client.markets()
        bases = [market.base_currency for market in buda_markets]
        quotes = [market.quote_currency for market in buda_markets]
        if from_currency in bases and to_currency in quotes:
            market = Market((from_currency, to_currency))
        elif from_currency in quotes and to_currency in bases:
            market = Market((to_currency, from_currency))
        else:
            raise ValueError(f'No compatible market found!')
        return market
