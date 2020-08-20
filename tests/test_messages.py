import unittest
import requests
import json
import os

class TestSchema(unittest.TestCase):
    def test_result(self):
        r = requests.get(f"http://{os.getenv('HOST', 'localhost')}:5000/messages")
        res = r.json()

        should_res = [
    "With the first link, the chain is forged. The first speech censured, the first thought forbidden, the first freedom denied - chains us all irrevocably. ",
    "Life's true gift is the capacity to enjoy SOMETHING.",
    "Commander William T. Riker: It's just that our mental pathways have become accustomed to your sensory input patterns. Lt. Commander Data: Hm. I understand. I am also fond of you, Commander. And you as well, Counselor.",
    "It's elementary, my dear Riker. Sir.",
    "Space... The final frontier. These are the voyages of the starship Enterprise. It's continuing mission, to explore strange new worlds. To seek out new life and new civilizations. To boldly go where no one has gone before.",
    "Darmok and Jalad at Tenagra!",
    "Let's make sure that history never forgets the name... Enterprise"]

        self.assertEqual(res, should_res, "Variables should be correctly replaced with their state or fallback")


if __name__ == "__main__":
    unittest.main()
