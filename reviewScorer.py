def scorer(restaurantString):
    sum = 0
    neg = 0
    neu = 0
    pos = 0
    compound = 0
    counter = 0
    print(get_review_text(spark_df, restaurantString))
    for x in get_review_text(spark_df, restaurantString):
        sum = sum + 1
        score = analyser.polarity_scores(x)
        for y in score.values():
            if (counter == 0):
                neg = neg + y
                counter = 1
            elif (counter == 1):
                neu = neu + y
                counter = 2
            elif (counter == 2):
                pos = pos + y
                counter = 3
            else:
                compound = compound + y
                counter = 0
    print("The aggregate negativity rating for " +  restaurantString + " is " + str(round(5 * neg/sum, 3)))
    print("The aggregate neutral rating for " +  restaurantString + " is " + str(round(5 * neu/sum, 3)))
    print("The aggregate positivity rating for " +  restaurantString + " is " + str(round(5 * pos/sum, 3)))
    print("The aggregate compound rating for " +  restaurantString + " is " + str(round(5 * compound/sum, 3)))
    print("The positivity to negativity ratio is " + str(round(pos/neg, 3)))
