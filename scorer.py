resArray = ['Sushi Osaka',
' Pokelava',
'Ling Ling Restaurant',
'DiBella’s Subs',
'Plantation Bar and Grill',
'Taco Bell',
'Temple of Zeus',
'Italian Carry Out',
'Domino’s Pizza',
'Sicilian Delight',
'Texas Roadhouse - Ithaca',
'Monks On The Commons',
'Kelly’s Dockside Cafe',
'Scale House Brew Pub',
'Northeast Pizza & Beer',
'Buffalo Wild Wings',
'Spring Buffet & Grill',
'Panera Bread',
' Denny’s',
'Firehouse Subs',
'Crossroads Bar & Grille',
'Sal’s Pizzeria',
'Louie’s Lunch',
'Coal Yard Cafe',
'Napoli Pizzeria',
'Inn at Taughannock Restaurant',
'Ithaca Bakery',
'Souvlaki House',
'Oishii Bowl',
'Asian Noodle House',
'Cafe Pacific',
'CoreLife Eatery',
'Chipotle Mexican Grill',
'Covered Bridge Mkt',
'Sahara Mediterranean Restaurant',
' Arby’s',
'Joe’s Restaurant',
'Mia Restaurant',
'Kilpatrick’s Publick House',
'Aladdin’s Natural Eatery',
'Lincoln Street Diner',
' McDonald’s',
' Mitsuba',
'Uncle Joe’s',
'The Rhine House',
'Ling Ling Garden',
'Ten Forward Cafe',
'Casablanca Pizzeria',
'Applebee’s Grill + Bar',
'Press Cafe',
'Moe’s Southwest Grill',
' Subway',
'Rogan’s Corner',
'Little Thai House',
'Miyake Japanese Restaurant',
'Friends & Pho',
'Salsa Fiesta',
'Mattin’s Café',
'Taverna Banfi',
'Ctb Appetizers',
'Circus Truck',
'Luna Inspired Street Food',
' Goldie’s',
'Dolce Delight',
'Sunset Grill',
'Plum Tree Restaurant',
'The Sub Shop',
'Danby Gathery',
' Okenshields',
'Amit Bhatia Libe Café',
'Universal Deli Grocery',
'Apollo Restaurant',
'D P Dough',
'Bibim Bap Korean Restaurant',
'Big Al’s Hilltop Quikstop',
'Taste of Thai',
'Easy Wok',
'Atrium Cafe',
'Little Caesars Pizza',
'Franny’s Food Truck',
'Tibetan Momo Bar',
'Waffle Frolic',
'Razorback BBQ',
'Purity Ice Cream',
'Royal Court Restaurant',
'The Ivy Room',
'Old Mexico',
'2nd Landing Cafe',
'Jason’s Grocery & Deli',
'Cup O’ Jo Café',
'Sinfully Delicious Baking Co.',
'Red & White Cafe',
' Trillium',
'Café Jennie',
'Pudgie’s Pizza & Sub Shops',
'Mama Teresa Pizzeria',
'Taste of Thai Express']

def synonyms(term):
    response = requests.get('http://www.thesaurus.com/browse/{}'.format(term))
    soup = BeautifulSoup(response.text, features = 'lxml')
    section = soup.find('section', {'class': 'synonyms-container'})
    return [span.text for span in section.findAll('span')]

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
        if votes == 0:
            votes = 1
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
    if neg!=0 or neg!=0.0 or neg!=0.00 or neg!=0.000:
        return round(pos/neg, 3)
    else:
        return pos
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
        if neg!=0 or neg!=0.0 or neg!=0.00 or neg!=0.000:
            return round(pos/neg, 3)
        else:
            return pos

def totalScores():
    scoreDict = {}
    for rest in resArray:
        scoreDict[rest] = scorer(rest)
    return scoreDict

def totalSpecificScore(aspect):
    scoreDict = {}
    for rest in resArray:
        scoreDict[rest] = specificScorer(rest, aspect)
    return scoreDict
