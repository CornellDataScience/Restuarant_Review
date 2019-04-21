"""
Returns positivity to negativity ratio given restaurant name "restaurantString"
"""
def scorer(restaurantString):
    sum = 0
    neg = 0
    pos = 0
    compound = 0
    counter = 0
    votes = 0
    rating = 0
    for y in get_num_votes(spark_df, restaurantString):
        votes = votes + y
    for x, y, a in zip(get_review_text(spark_df, restaurantString), get_num_votes(spark_df, restaurantString), get_rating(spark_df, restaurantString)):
        sum = sum + 1
        rating = rating + a
        score = analyser.polarity_scores(x)
        for z in score.values():
            if (counter == 0):
                neg = neg + sqrt((100*y)/votes)*z
                counter = 1
            elif (counter == 1):
                counter = 2
            elif (counter == 2):
                pos = pos + sqrt((100*y)/votes)*z
                counter = 3
            else:
                compound = compound + sqrt((100*y)/votes)*z
                counter = 0
    return round(pos/neg, 3)
    #print("The average rating for " +  restaurantString + " is " + str(round(rating/sum, 3)))

"""
Returns positivity to negativity ratio given restaurant name "restaurantString" and
aspect to analyze "aspect"
"""
def specificScorer(restaurantString, aspect):
    synonym = synonyms(aspect)
    reviews = get_review_text(spark_df, restaurantString)
    sentencesList = []
    b=[]

    for r in reviews:
        sentencesList.append(r.split('. '))
    for z in sentencesList:
        for sentence in z:
            for word in synonym:
                if word in sentence:
                    b.append(sentence)
                    z.remove(sentence)
                    break

    counter = 0
    neg = 0
    pos = 0
    length = len(b)
    if (length == 0):
        return "There are no reviews corresponding to the " + aspect + " of " + restaurantString
    else:
        for x in b:
            score = analyser.polarity_scores(x)
            for z in score.values():
                if (counter == 0):
                    neg = neg + z
                    counter = 1
                elif (counter == 1):
                    counter = 2
                elif (counter == 2):
                    pos = pos + z
                    counter = 3
                else:
                    counter = 0
        if neg!=0:
            return round(pos/neg, 3)
