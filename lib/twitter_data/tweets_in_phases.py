from lib.twitter_data.tweets import Tweets
from typing import List
import ruptures as rpt
from datetime import datetime


class TweetsInPhases(Tweets):
    def __init__(self, tweets):
        super().__init__(tweets)

        # TODO is using only n-1 attributes for each category correct to avoid multicollinearity?
        VARIABLES_FOR_BREAKPOINT_ANALYSIS = [
            "retweet_pct",
            "original_tweet_pct",
            "reply_pct",
            "laggards_pct",
            "active_pct",
        ]

        self._breakpoints = self.get_breakpoints(VARIABLES_FOR_BREAKPOINT_ANALYSIS)

        self._phases = self._split_tweets_at_breakpoints()

    def __len__(self) -> int:
        return len(self._phases)

    @property
    def breakpoints(self) -> List[datetime]:
        return self._breakpoints

    @property
    def phases(self) -> List[Tweets]:
        return self._phases

    def get_breakpoints(self, analysis_vars: List[str]) -> List[datetime]:
        """Returns the breakpoints in the twitter time_series based on the analysis_vars specified

        :param analysis_vars: variables to take into consideration for calculating the breakpoints
        (e.g. ['retweet_pct', 'reply_pct', 'de_pct'....]
        :return: list of breakpoints; Breakpoint at the end of the dataset (i.e. at last position) is not included
        """

        signal = self.hourwise_metrics[analysis_vars].to_numpy().astype("float64")

        # Specify the model params
        algo = rpt.Pelt(model="rbf").fit(signal)
        # TODO tweak penalty
        result = algo.predict(pen=3)

        # result from ruptures typically contains one breakpoint all the way after the dataset, e.g. [205, 480] for a
        bkp_in_data = [x - 1 for x in result]
        return [x.to_pydatetime() for x in self.hourwise_metrics.index[bkp_in_data]]

    def _split_tweets_at_breakpoints(self) -> List[Tweets]:
        """Splits the tweets at their breakpoints into phases

        :return: list of Tweet objs, where each obj contains all tweets in a phase
        """

        breakpoints = self.breakpoints
        # last breakpoints seems to be irrelevant as it is just the last tweet
        breakpoints.pop()

        # Add one breakpoints all the way in the beginning and one all the way in the end
        breakpoints_wrapped = (
            [self.tweets["hour"].min().to_pydatetime()]
            + breakpoints
            + [self.tweets["hour"].max().to_pydatetime()]
        )

        return [
            self.select_tweets_in_time_range(
                breakpoints_wrapped[i], breakpoints_wrapped[i + 1], "hour"
            )
            for i in range(len(breakpoints_wrapped) - 1)
        ]
