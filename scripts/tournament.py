import math

class Participant:
    """
    The Participant class represents a participant in a specific match.
    It can be used as a placeholder until the participant is decided.
    """

    def __init__(self, competitor=None):
        self.competitor = competitor

    def get_competitor(self):
        """
        Return the competitor that was set,
        or None if it hasn't been decided yet
        """
        return self.competitor

    def set_competitor(self, competitor):
        """
        Set competitor after you've decided who it will be,
        after a previous match is completed.
        """
        self.competitor = competitor


class Match:
    """
    A match represents a single match in a tournament, between 2 participants.
    It adds empty participants as placeholders for the winner and loser,
    so they can be accessed as individual object pointers.
    """

    def __init__(self, left_participant, right_participant):
        self.__left_participant = left_participant
        self.__right_participant = right_participant
        self.__winner = Participant()
        self.__loser = Participant()

    def set_winner(self, competitor):
        """
        When the match is over, set the winner competitor here and the loser will be set too.
        """
        if competitor == self.__left_participant.get_competitor():
            self.__winner.set_competitor(competitor)
            self.__loser.set_competitor(self.__right_participant.get_competitor())
        elif competitor == self.__right_participant.get_competitor():
            self.__winner.set_competitor(competitor)
            self.__loser.set_competitor(self.__left_participant.get_competitor())
        else:
            raise Exception("Invalid competitor")

    def get_winner_participant(self):
        """
        If the winner is set, get it here. Otherwise this return None.
        """
        return self.__winner

    def get_loser_participant(self):
        """
        If the winner is set, you can get the loser here. Otherwise this return None.
        """
        return self.__loser

    def get_participants(self):
        """
        Get the left and right participants in a list.
        """
        return [self.__left_participant, self.__right_participant]

    def is_ready_to_start(self):
        """
        This returns True if both of the participants coming in have their competitors "resolved".
        This means that the match that the participant is coming from is finished.
        It also ensure that the winner hasn't been set yet.
        """
        is_left_resolved = self.__left_participant.get_competitor() is not None
        is_right_resolved = self.__right_participant.get_competitor() is not None
        is_winner_resolved = self.__winner.get_competitor() is not None
        return is_left_resolved and is_right_resolved and not is_winner_resolved


class Tournament:
    """
    This is a single-elimination tournament where each match is between 2 competitors.
    It takes in a list of competitors, which can be strings or any type of Python object,
    but they should be unique. They should be ordered by a seed, with the first entry being the most
    skilled and the last being the least. They can also be randomized before creating the instance.
    Optional options dict fields:
    """

    def __init__(self, competitors_list, options={}):
        assert len(competitors_list) > 1
        self.__matches = []
        next_higher_power_of_two = int(
            math.pow(2, math.ceil(math.log2(len(competitors_list))))
        )
        winners_number_of_byes = next_higher_power_of_two - len(competitors_list)
        incoming_participants = list(map(Participant, competitors_list))
        incoming_participants.extend([None] * winners_number_of_byes)
        num_of_rounds = int(math.ceil(math.log2(len(incoming_participants))))

        round_ = 1
        while round_ <= num_of_rounds:
            if round_ == 1:
                half_length = int(len(incoming_participants) / 2)
                first = incoming_participants[0:half_length]
                last = incoming_participants[half_length:]
                last.reverse()
                round_1_winners = []
                round_1_losers = []
                for participant_pair in zip(first, last):
                    if participant_pair[1] is None:
                        round_1_winners.append(participant_pair[0])
                    elif participant_pair[0] is None:
                        round_1_winners.append(participant_pair[1])
                    else:
                        match = Match(participant_pair[0], participant_pair[1])
                        round_1_winners.append(match.get_winner_participant())
                        round_1_losers.append(match.get_loser_participant())
                        self.__matches.append(match)
                next_round = [round_1_winners, round_1_losers]
                round_ += 1

            elif round_ > 1:
                while True:
                    if len(next_round[0]) > 1:
                        bracket = next_round[0]
                        half_length = int(len(bracket) / 2)
                        first = bracket[0:half_length]
                        last = bracket[half_length:]
                        last.reverse()
                        next_round_winners = []
                        next_round_losers = []
                        for participant_pair in zip(first, last):
                            if participant_pair[1] is None:
                                next_round_winners.append(participant_pair[0])
                            elif participant_pair[0] is None:
                                next_round_winners.append(participant_pair[1])
                            else:
                                match = Match(participant_pair[0], participant_pair[1])
                                next_round_winners.append(match.get_winner_participant())
                                next_round_losers.append(match.get_loser_participant())
                                self.__matches.append(match)
                        try:
                            next_round.remove(bracket)
                        except:
                            continue
                        next_round.append(next_round_winners)
                        next_round.append(next_round_losers)
                        next_round = [ele for ele in next_round if ele != []]
                        if len(next_round) == len(competitors_list):
                            break
                    round_ += 1

        self.__final = next_round

    def __iter__(self):
        return iter(self.__matches)

    def get_active_matches(self):
        """
        Returns a list of all matches that are ready to be played.
        """
        return [match for match in self.get_matches() if match.is_ready_to_start()]

    def get_matches(self):
        """
        Returns a list of all matches for the tournament.
        """
        return self.__matches

    def get_active_matches_for_competitor(self, competitor):
        """
        Given the string or object of the competitor that was supplied
        when creating the tournament instance,
        returns a list of Matches that they are currently playing in.
        """
        matches = []
        for match in self.get_active_matches():
            competitors = [
                participant.get_competitor() for participant in match.get_participants()
            ]
            if competitor in competitors:
                matches.append(match)
        return matches

    def get_final(self):
        """
        Returns None if the winner has not been decided yet,
        and returns a list containing the single victor otherwise.
        """
        if len(self.get_active_matches()) > 0:
            return None

        final_dict = {}
        for team in range(len(self.__final)):
            final_dict[f'{team+1}'] = self.__final[team][0].get_competitor()

        return final_dict

    def add_win(self, match, competitor):
        """
        Set the victor of a match, given the competitor string/object and match.
        """
        match.set_winner(competitor)