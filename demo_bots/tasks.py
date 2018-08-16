from trading_bots.bots import BotTask


def run_bot(event):
    event_kwargs = event.get('kwargs', {})
    bot = event_kwargs['label']
    config = event_kwargs.get('config')
    bot_task = BotTask(bot, config, None)
    bot_task.run_once()
