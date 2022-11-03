from faker import Faker
from datetime import date


class DataGenerator(Faker):
    fake = Faker()

    def sample_associado(self, row_number):
        data = list()

        for row in range(1, row_number + 1):
            teste = {
                'id': self.fake.unique.random_int(min=1, max=row_number),
                'nome': self.fake.first_name(),
                'sobrenome': self.fake.last_name(),
                'idade': self.fake.random_int(min=18, max=100),
                'email': self.fake.email()
            }

            data.append(teste)

        return data

    def sample_conta(self, row_number):
        data = list()

        for row in range(1, row_number + 1):
            teste = {
                'id': row,
                'tipo': self.fake.pystr(max_chars=5),
                'data_criacao': self.fake.date_time_between(start_date=date(2020, 1, 1), end_date=date(2022, 1, 1)),
                'id_associado': row
            }

            data.append(teste)

        return data

    def sample_cartao(self, row_number):
        data = list()

        for row in range(1, row_number + 1):
            teste = {
                'id': row,
                'num_cartao': self.fake.random_int(),
                'nom_impresso': self.fake.name(),
                'data_criacao': self.fake.date_time_between(start_date=date(2020, 1, 1), end_date=date(2022, 1, 1)),
                'id_conta': row,
                'id_associado': row
            }

            data.append(teste)

        return data

    def sample_movimento(self, row_number):
        data = list()

        for row in range(1, row_number + 1):
            teste = {
                'id': row,
                'vlr_transacao': self.fake.pyfloat(min_value=1, max_value=10000, right_digits=2),
                'des_transacao': self.fake.pystr(max_chars=5),
                'data_movimento': self.fake.date_time_between(start_date=date(2020, 1, 1), end_date=date(2022, 1, 1)),
                'id_cartao': row
            }

            data.append(teste)

        return data
