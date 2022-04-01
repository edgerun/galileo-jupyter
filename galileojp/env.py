from pathlib import Path

import dotenv


def load():
    home = Path.home()

    p = home.joinpath('.galileojp')
    if p.is_file():
        dotenv.load_dotenv(str(p))
