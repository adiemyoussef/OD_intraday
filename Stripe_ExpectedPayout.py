import stripe
from datetime import datetime, timedelta
import concurrent.futures

#TODO: Store Stripe Api Safely


# List of test accounts to exclude
TEST_ACCOUNTS = ['cocoleboss', 'adiem', 'elbekri']

def is_test_account(customer_id):
    pass
    # customer = stripe.Customer.retrieve(customer_id)
    # for test_account in TEST_ACCOUNTS:
    #     if test_account in customer.email:
    #         return True
    # return False

def check_trial_end(subscription, today, one_day, three_days, seven_days):
    trial_end = datetime.fromtimestamp(subscription['trial_end'])
    amount = subscription['plan']['amount'] / 100  # Convert amount from cents to dollars
    next_day = today <= trial_end < one_day
    next_3_days = today <= trial_end < three_days
    next_7_days = today <= trial_end < seven_days
    return next_day, next_3_days, next_7_days, amount

def check_active_subscription(subscription, today, one_day, three_days, seven_days):
    current_period_end = datetime.fromtimestamp(subscription['current_period_end'])
    amount = subscription['plan']['amount'] / 100  # Convert amount from cents to dollars
    next_day = today <= current_period_end < one_day
    next_3_days = today <= current_period_end < three_days
    next_7_days = today <= current_period_end < seven_days
    return next_day, next_3_days, next_7_days, amount

def check_trial_to_cancel(subscription, today, one_day, three_days):
    canceled_at = subscription.get('canceled_at')
    if canceled_at:
        canceled_at_date = datetime.fromtimestamp(canceled_at)
        if today - timedelta(days=1) <= canceled_at_date < today:
            return 1, 0
        elif today - timedelta(days=3) <= canceled_at_date < today:
            return 0, 1
    return 0, 0

def get_trial_ending_counts():
    today = datetime.now()
    one_day = today + timedelta(days=3)
    three_days = today + timedelta(days=7)
    seven_days = today + timedelta(days=10)

    subscriptions = stripe.Subscription.list(status='trialing', limit=100)

    trial_ending_next_day = 0
    trial_ending_next_3_days = 0
    trial_ending_next_7_days = 0
    amount_next_day = 0
    amount_next_3_days = 0
    amount_next_7_days = 0

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(check_trial_end, subscription, today, one_day, three_days, seven_days)
            for subscription in subscriptions.auto_paging_iter()
            if not is_test_account(subscription['customer'])
        ]

        for future in concurrent.futures.as_completed(futures):
            next_day, next_3_days, next_7_days, amount = future.result()
            if next_day:
                trial_ending_next_day += 1
                amount_next_day += amount
            if next_3_days:
                trial_ending_next_3_days += 1
                amount_next_3_days += amount
            if next_7_days:
                trial_ending_next_7_days += 1
                amount_next_7_days += amount

    return (trial_ending_next_day, amount_next_day), (trial_ending_next_3_days, amount_next_3_days), (trial_ending_next_7_days, amount_next_7_days)

def get_active_subscription_counts():
    today = datetime.now()
    one_day = today + timedelta(days=3)
    three_days = today + timedelta(days=7)
    seven_days = today + timedelta(days=10)

    subscriptions = stripe.Subscription.list(status='active', limit=100)

    active_next_day = 0
    active_next_3_days = 0
    active_next_7_days = 0
    amount_active_next_day = 0
    amount_active_next_3_days = 0
    amount_active_next_7_days = 0

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(check_active_subscription, subscription, today, one_day, three_days, seven_days)
            for subscription in subscriptions.auto_paging_iter()
            if not is_test_account(subscription['customer'])
        ]

        for future in concurrent.futures.as_completed(futures):
            next_day, next_3_days, next_7_days, amount = future.result()
            if next_day:
                active_next_day += 1
                amount_active_next_day += amount
            if next_3_days:
                active_next_3_days += 1
                amount_active_next_3_days += amount
            if next_7_days:
                active_next_7_days += 1
                amount_active_next_7_days += amount

    return (active_next_day, amount_active_next_day), (active_next_3_days, amount_active_next_3_days), (active_next_7_days, amount_active_next_7_days)

def get_trial_to_cancel_counts():
    today = datetime.now()
    one_day = today - timedelta(days=3)
    three_days = today - timedelta(days=10)

    subscriptions = stripe.Subscription.list(status='canceled', limit=100)

    canceled_last_day = 0
    canceled_last_3_days = 0

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(check_trial_to_cancel, subscription, today, one_day, three_days)
            for subscription in subscriptions.auto_paging_iter()
            if not is_test_account(subscription['customer'])
        ]

        for future in concurrent.futures.as_completed(futures):
            last_day, last_3_days = future.result()
            canceled_last_day += last_day
            canceled_last_3_days += last_3_days

    return canceled_last_day, canceled_last_3_days

if __name__ == '__main__':
    # Get the counts and amounts for trial ending
    next_day, next_3_days, next_7_days = get_trial_ending_counts()
    active_next_day, active_next_3_days, active_next_7_days = get_active_subscription_counts()

    print(f"Number of trial periods ending in the next 3 day: {next_day[0]}, Expected amount: ${next_day[1]:.2f}")
    print(f"Number of trial periods ending in the next 7 days: {next_3_days[0]}, Expected amount: ${next_3_days[1]:.2f}")
    print(f"Number of trial periods ending in the next 10 days: {next_7_days[0]}, Expected amount: ${next_7_days[1]:.2f}")

    print(f"Number of active subscriptions renewing in the next 3 day: {active_next_day[0]}, Expected amount: ${active_next_day[1]:.2f}")
    print(f"Number of active subscriptions renewing in the next 7 days: {active_next_3_days[0]}, Expected amount: ${active_next_3_days[1]:.2f}")
    print(f"Number of active subscriptions renewing in the next 10 days: {active_next_7_days[0]}, Expected amount: ${active_next_7_days[1]:.2f}")

    # Get the counts for trial to cancel
    canceled_last_day, canceled_last_3_days = get_trial_to_cancel_counts()

    print(f"Number of customers who went from trial to cancel in the last day: {canceled_last_day}")
    print(f"Number of customers who went from trial to cancel in the last 3 days: {canceled_last_3_days}")