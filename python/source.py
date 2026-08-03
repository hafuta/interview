"""The Source: produces the raw events to be collected."""

from __future__ import annotations

from event import Event

# Account distribution for the generated stream: events are spread across a set
# of accounts, with one account ("acct-hot") receiving the majority.
HOT_ACCOUNT = "acct-hot"
HOT_ACCOUNT_SHARE = 90  # percent of events that belong to the hot account
COLD_ACCOUNTS = ["acct-a", "acct-b", "acct-c", "acct-d"]
RESOURCES_PER_ACCOUNT = 8


def account_for(i: int) -> str:
    """Decide which account event i belongs to."""
    if i % 100 < HOT_ACCOUNT_SHARE:
        return HOT_ACCOUNT
    return COLD_ACCOUNTS[i % len(COLD_ACCOUNTS)]


def resource_for(account: str, i: int) -> str:
    """Decide which resource within the account event i touches.

    Each account has a handful of resources.
    """
    return f"{account}/res-{i % RESOURCES_PER_ACCOUNT:02d}"


def new_event(i: int) -> Event:
    """Build event i with its account and a derived resource."""
    account = account_for(i)
    return Event(
        id=i,
        account_id=account,
        resource_id=resource_for(account, i),
        payload=f"resource-{i:06d}",
    )


class Source:
    """Produces the raw events to be collected.

    In the real system this is a Pub/Sub subscription. Here it is just an
    in-memory generator so the exercise has zero external dependencies.
    """

    def __init__(self, count: int) -> None:
        self.count = count

    def events(self) -> list[Event]:
        """Return all events as a list."""
        return [new_event(i) for i in range(self.count)]
