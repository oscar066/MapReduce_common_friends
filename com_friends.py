
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# A map reduce program to find common friends for each pair of users
# Input: a list of (user_id, friend_id) pairs
# Output: a list of (user_id, friend_id) pairs with common friends


class MutualFriends(MRJob):
    def mapper(self, _, line):
        # split the line into user and friends
        user, friends = line.split(',')
        # split the friends into a list
        friends = friends.split(' ')
        # for each friend, emit a key value pair
        for friend in friends:
            # if the user is the same as the friend, skip
            if user == friend:
                continue
            # sort the user and friend 
            userKey = (int(user) < int(friend)) and user + ',' + friend or friend + ',' + user
            regex = '(\b' + friend + '[^\w]+)|\b,?' + friend + '$'
            # emit the key value pair
            yield userKey, re.sub(regex, '', line)

    def reducer(self, key, values):
        friendsList = []
        # for each value, append it to the list of friends
        for value in values:
            friendsList.append(value)
        # if there are two values, then there are two friends
        if len(friendsList) == 2:
            firstList = friendsList[0].split(',')[1].split(' ')
            secondList = friendsList[1].split(',')[1].split(' ')
            # find the mutual friends
            # convert the lists to sets and find the intersection
            mutualFriends = list(set(firstList) & set(secondList))
            # if there are mutual friends, emit the key value pair
            if len(mutualFriends) != 0:
                yield key, [' '.join(mutualFriends)]
            elif len(mutualFriends) == 0:
                yield key, ['No mutual friends']

    def steps(self):
        # return the steps
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

if __name__ == '__main__':
    MutualFriends.run()
