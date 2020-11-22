# %%
from collections.abc import Iterable as AbstractIterable
import csv
from dataclasses import dataclass, asdict, field
from datetime import date, datetime, time
from io import StringIO
from itertools import product
from typing import Iterable, List, Dict, Union, Iterator, Tuple, Optional
from bs4 import BeautifulSoup
import requests
from .classes import Point
from . import connection

# %%
settings = {
    "reference_url": "http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sih/cnv/nibr.def",
    "data_url": "http://tabnet.datasus.gov.br/cgi/tabcgi.exe?sih/cnv/nibr.def",
    "series_prefix": "SUS\\Morbidade Hospitalar"
}

# %%
def process():
    for year, month in product(range(2008, 2020 + 1), range(1, 12 + 1)):
        print(f'Scraping {year}/{month}')
        dt = date(year, month, 1)
        page = Page([dt])
        if not page.csv:
            print('No data found in page')
            continue
        for row in make_csv_reader(page.csv):
            for series in CSVRowAdapter(row, datetime.combine(dt, time(0))):
                connection.add_point(series.uid, series.point)
                connection.add_fields(series.uid, asdict(series.fields))
        print('Saved series')
    return True


# %%
class Page:

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:82.0) Gecko/20100101 Firefox/82.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'http://tabnet.datasus.gov.br',
        'Connection': 'keep-alive',
        'Referer': 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sih/cnv/nibr.def',
        'Upgrade-Insecure-Requests': '1',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache'
    }

    decoded_base_data = {
        'Linha': 'Município',
        'Coluna': 'Faixa_Etária_2',
        'Incremento': 'Internações',
        'pesqmes1': 'Digite o texto e ache fácil',
        'SMunicípio': 'TODAS_AS_CATEGORIAS__',
        'pesqmes2': 'Digite o texto e ache fácil',
        'SCapital': 'TODAS_AS_CATEGORIAS__',
        'pesqmes3': 'Digite o texto e ache fácil',
        'SRegião_de_Saúde_(CIR)': 'TODAS_AS_CATEGORIAS__',
        'pesqmes4': 'Digite o texto e ache fácil',
        'SMacrorregião_de_Saúde': 'TODAS_AS_CATEGORIAS__',
        'pesqmes5': 'Digite o texto e ache fácil',
        'SMicrorregião_IBGE': 'TODAS_AS_CATEGORIAS__',
        'pesqmes6': 'Digite o texto e ache fácil',
        'SRegião_Metropolitana_-_RIDE': 'TODAS_AS_CATEGORIAS__',
        'pesqmes7': 'Digite o texto e ache fácil',
        'STerritório_da_Cidadania': 'TODAS_AS_CATEGORIAS__',
        'pesqmes8': 'Digite o texto e ache fácil',
        'SMesorregião_PNDR': 'TODAS_AS_CATEGORIAS__',
        'SAmazônia_Legal': 'TODAS_AS_CATEGORIAS__',
        'SSemiárido': 'TODAS_AS_CATEGORIAS__',
        'SFaixa_de_Fronteira': 'TODAS_AS_CATEGORIAS__',
        'SZona_de_Fronteira': 'TODAS_AS_CATEGORIAS__',
        'SMunicípio_de_extrema_pobreza': 'TODAS_AS_CATEGORIAS__',
        'SCaráter_atendimento': 'TODAS_AS_CATEGORIAS__',
        'SRegime': 'TODAS_AS_CATEGORIAS__',
        'pesqmes16': 'Digite o texto e ache fácil',
        'SCapítulo_CID-10': 'TODAS_AS_CATEGORIAS__',
        'pesqmes17': '',
        'SLista_Morb__CID-10': '148',
        'pesqmes18': 'Digite o texto e ache fácil',
        'SFaixa_Etária_1': 'TODAS_AS_CATEGORIAS__',
        'pesqmes19': 'Digite o texto e ache fácil',
        'SFaixa_Etária_2': ['6', '7', '8'],
        'SSexo': '2',
        'SCor/raça': 'TODAS_AS_CATEGORIAS__',
        'formato': 'prn',
        'mostre': 'Mostra'
    }

    encoding = 'latin1'

    def __init__(self, dates: Iterable[date]):
        decoded_files_data = {'Arquivos': self.parse_dates_as_filenames(dates)}
        decoded_data = {**self.decoded_base_data, **decoded_files_data}
        self.encoded_data = self.encode_data(decoded_data)
        response = requests.post(settings['data_url'], headers=self.headers, data=self.encoded_data, timeout=120)
        response.raise_for_status()
        print('Got response')
        self.response = response

    def encode_data(self, data: Dict[str, Union[str, List[str]]]) -> Dict[bytes, Union[bytes, List[bytes]]]:
        r: Dict[bytes, Union[bytes, List[bytes]]] = {}
        for k, v in data.items():
            encoded_k = k.encode(self.encoding)
            if isinstance(v, str):
                r[encoded_k] = v.encode(self.encoding)
            elif isinstance(v, list):
                r[encoded_k] = [el.encode(self.encoding) for el in v]
            else:
                raise TypeError('Expected values in data to be either str or list')
        return r

    @staticmethod
    def parse_dates_as_filenames(dates: Iterable[date]) -> List[str]:
        as_year_and_month = (dt.strftime('%y%m') for dt in dates)
        return [f'nibr{dt}.dbf' for dt in as_year_and_month]

    @property
    def csv(self) -> Optional[str]:
        soup = BeautifulSoup(self.response.text, features='lxml')
        csv_tag = soup.find('pre')
        if csv_tag:
            assert csv_tag.string[:2] == '\r\n'
            return csv_tag.string[2:]
        else:
            return None


def make_csv_reader(csv_str: str) -> csv.DictReader:
    stream = StringIO(csv_str)
    return csv.DictReader(stream, delimiter=';')


@dataclass
class Fields:
    município_nom: str
    faixa_etária: str
    município_cod: Optional[str]
    morbidade: str = 'Epilepsia'
    sexo: str = 'Feminino'
    source: str = 'SUS'
    variable: str = 'Morbidade Hospitalar'
    description: str = field(init=False)

    def __post_init__(self):
        self.description = f'SUS - Morbidade, Epilepsia, Feminino, {self.faixa_etária}, {self.município_nom}'


@dataclass
class Series:
    uid: str
    point: Point
    fields: Fields

    def __post_init__(self):
        self.uid = '\\'.join([settings['series_prefix'], self.uid])


class CSVRowAdapter(AbstractIterable):

    city_key = 'Município'

    def __init__(self, row: Dict[str, str], datetime: datetime):
        self.row = row
        self.dt = datetime
        self.city_code, self.city_name = self.extract_city_code_and_name(self.row[self.city_key])

    @staticmethod
    def extract_city_code_and_name(city_value: str) -> Tuple[Optional[str], str]:
        """Expects a string like '530010 Rio de Janeiro' or 'Total'."""
        poss_code, *poss_name = city_value.split(' ')
        if poss_code == 'Total':
            return None, poss_code
        else:
            return poss_code, ' '.join(poss_name)

    def __iter__(self) -> Iterator[Series]:
        return (
            Series(f'{Fields.morbidade}\\{Fields.sexo}\\{self.city_name}\\{k}',
                   Point(self.dt, float(v)),
                   Fields(self.city_name, k, self.city_code))
            for k, v in self.row.items()
            if k != self.city_key and v and v != '-'
        )


# %%
def test():
    import unittest

    class Test(unittest.TestCase):

        def test_parse_dates_as_filenames(self):
            input = [date(2020, 1, 1), date(2020, 3, 1)]
            output = ['nibr2001.dbf', 'nibr2003.dbf']
            self.assertEqual(Page.parse_dates_as_filenames(input), output)

        def test_extract_city_code_and_name(self):
            test = {
                '111 Br': ('111', 'Br'),
                '111 Rio de Janeiro': ('111', 'Rio de Janeiro'),
                'Total': (None, 'Total')
            }
            for input_, output in test.items():
                self.assertEqual(CSVRowAdapter.extract_city_code_and_name(input_), output)

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(Test)
    unittest.TextTestRunner().run(suite)
    return True

if __name__ == '__main__':
    from sys import argv

    globals()[argv[1]]()