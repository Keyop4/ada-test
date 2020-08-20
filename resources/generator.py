import sqlite3
import json
from pprint import pprint
from uuid import uuid4

def text(text):
    return {"type": "text", "body": text}


t = text


def random(content):
    return {"type": "random", "body": content}


r = random


def image(url, alt=None):
    x = {"type": "image", "url": url}
    if alt:
        x["alt-text"] = alt
    return x


i = image


def http(url, success, failure):
    return {"type": "http", "success": success, "failure": failure}


h = http


def maybe(chance, body):
    return {"type": "maybe", "chance": chance, "body": body}


m = maybe


def answer(title, content):
    return {"title": title, "content": content}


a = answer

def wait(time):
    return {"type": "wait", "wait-time": time}

a1 = a(
    "These are the voyages",
    [
        t(x)
        for x in "These are the voyages of the Starship Enterprise. Its continuing mission, to explore strange new worlds. To seek out new life and new civilizations. To boldly go where no one has gone before.".split(
            "."
        )
    ],
)

a2 = a(
    "Data on friendship",
    [
        *[
            t(x)
            for x in "I never knew what a friend was until I met Geordi. He spoke to me as though I were human. He treated me no differently from anyone else. He accepted me for what I am. And that, I have learned, is friendship.".split(
                "."
            )
        ],
        i(
            "http://vignette.wikia.nocookie.net/memoryalpha/images/4/4f/Data%2C_2366.jpg/revision/latest?cb=20130529102644&path-prefix=en",
        ),
    ],
)

a3 = a(
    "Star Trek API",
    [
        t("Api call time!"),
        http(
            "http://stapi.co/api/v1/rest/character/search?pageNumber=0&pageSize=100",
            [
                t("Made it so!"),
                i(
                    "https://treknews.net/wp-content/uploads/2011/05/2.jpg",
                    "picard thumbs up",
                ),
            ],
            [
                random(
                    [
                        i(
                            "https://d13ezvd6yrslxm.cloudfront.net/wp/wp-content/images/startrek-picard-facepalm-700x341.jpg",
                            "picard facepalm",
                        ),
                        i(
                            "https://www.everseradio.com/wp-content/uploads/2009/06/facepalm.png",
                            "picard double facepalm",
                        ),
                    ]
                )
            ],
        ),
    ],
)

a4 = a(
    "Maybe Riker",
    [
        maybe(
            0.5,
            [
                i(
                    "https://ca.startrek.com/sites/default/files/styles/content_full/public/images/2019-07/18ead4c77c3f40dabf9735432ac9d97a.jpg",
                    "Riker with beard",
                )
            ],
        )
    ],
)

a5 = a(
    "Maybe Data with Beard",
    [
        t("Forbidden knowledge!"),
        maybe(
            0.3,
            [
                i(
                    "https://vignette.wikia.nocookie.net/memoryalpha/images/7/7e/Data_wearing_a_beard.jpg/revision/latest?cb=20121212024612&path-prefix=en"
                )
            ],
        ),
    ],
)

a6 = a(
    "Borg Hails",
    [
        r(
            [
                t("You will be assimilated. Resistance is futile."),
                t(
                    "We are the Borg. Lower your shields and surrender your ships. We will add your biological and technological distinctiveness to our own. Your culture will adapt to service us. Resistance is futile."
                ),
                t(
                    "We are the Borg. Your biological and technological distinctiveness will be added to our own. Resistance is futile."
                ),
            ]
        ),
        r(
            [
                i(
                    "https://vignette.wikia.nocookie.net/memoryalpha/images/4/48/Picard_dreams_his_assimilation.jpg/revision/latest?cb=20120317185405&path-prefix=en"
                ),
                i(
                    "https://vignette.wikia.nocookie.net/memoryalpha/images/4/40/Hugh-Drone.jpg/revision/latest?cb=20141207193522&path-prefix=en",
                    "Hugh",
                ),
            ]
        ),
    ],
)

a7 = a(
    "Data or Spock?",
    [
        t("Data or Spock?"),
        t("<drum roll>"),
        h(
            "http://api.coinflip.com",
            [
                i(
                    "https://cdn1.thr.com/sites/default/files/imagecache/768x433/2018/02/star_trek_tv_spock_3_copy_-_h_2018.jpg",
                    "Spock!",
                )
            ],
            [i("https://i.stack.imgur.com/JwRfI.png"), wait(42)],
        ),
    ],
)
a8 = a("Not Picard! Not anyone, in fact", [])

answers = [a1, a2, a3, a4, a5, a6, a7, a8]


messages = [
    "With the {74c695031a554c2ebfdb2ee123c8b4f6|something} link, the chain is forged. The {74c695031a554c2ebfdb2ee123c8b4f6|} speech censured, the {74c695031a554c2ebfdb2ee123c8b4f6|} thought forbidden, the {74c695031a554c2ebfdb2ee123c8b4f6|} freedom denied - chains us all irrevocably. ",
    "Life's true gift is the capacity to enjoy {6b0f3753e17d42598a6b2b8468e3c20f|SOMETHING}.",
    "Commander William T. {4b0f3753e17d42598a6b2b8468e3b19e|}: It's just that our mental pathways have become accustomed to your sensory input patterns. Lt. Commander Data: Hm. I understand. I am also fond of you, Commander. And you as well, Counselor.",
    "It's elementary, my dear {4b0f3753e17d42598a6b2b8468e3b19e|Watson}. Sir.",
    "Space... The final frontier. These are the voyages of the starship {f88845ecb8794308af2ecbb663ecf667|}. It's continuing mission, to explore strange new worlds. To seek out new life and new civilizations. To boldly go where no one has gone before.",
    "{b8db1b3a29454bafbed460306e7f8318|some guy} and {9926d7be6bb44850bf34d1f7cc3c2018|another guy} at Tenagra!",
    "Let's make sure that history never forgets the name... {f88845ecb8794308af2ecbb663ecf667|}",
    ]

variables = {"first": "74c695031a554c2ebfdb2ee123c8b4f6",
    "Riker": "4b0f3753e17d42598a6b2b8468e3b19e",
    "Enterprise": "f88845ecb8794308af2ecbb663ecf667",
    "Darmok": "b8db1b3a29454bafbed460306e7f8318",
    "Red Herring": "c9fdac3a29454bafbed460306e7f1111",
    "Jalad": "9926d7be6bb44850bf34d1f7cc3c2018"
}


conn = sqlite3.connect("../database.db")

try:
    conn.execute("drop table answers")
    conn.execute("drop table blocks")
    conn.execute("drop table messages")
    conn.execute("drop table state")
except:
    pass
conn.execute("create table answers (id integer primary key, title text)")
conn.execute(
    "create table blocks (id integer primary key, content text, answer_id integer not null, foreign key (answer_id) references answers (id))"
)
conn.execute("create table messages (id integer primary key, body text)")
conn.execute("create table state (id text primary key, value text)")

for i in range(0, 8):
    conn.execute(
        "insert into answers(id, title) values (?,?)", (i + 1, answers[i]["title"])
    )
    conn.execute(
        "insert into blocks(id, content, answer_id) values (?,?, ?)",
        (i + 1, json.dumps(answers[i]["content"]), i + 1),
    )
    print(i)

for m in messages:
    conn.execute("insert into messages(body) values(?)",[m])

for val,id in variables.items():
    conn.execute("insert into state(id,value) values(?, ?)",[id,val])


conn.commit()
conn.close()

"""

https://dbdiagram.io/d


Table answers as a {
  id int [pk, increment] // auto-increment
  title text
}

Table blocks {
  id int [pk]
  content text
  answer_id int [ref: - a.id] // inline relationship (many-to-one)
}

Table messages {
  id int [pk]
  body text
}


Table state {
  id text [pk]
  value text
}

"""
