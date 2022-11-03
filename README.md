# sicredi_data_engineering_challenge


Repositório com a solução para o desafio técnico de data engineer Sicredi.

Neste desafio, o intuíto foi de modelar bases de dados em um banco SQL, inserir uma massa de dados nessas tabelas, e a partir desses dados já persistidos, coletá-los para criar uma nova visão dos dados agrupados, disponibilizando-os em um único arquivo CSV.  

A ideia de arquitetura foi primeiramente, criar as tabelas necessárias em um banco de dados Postgres, utilizando a biblioteca SQLAlchemy como sistema de ORM. Após essas tabelas estarem devidamente modeladas e criadas no banco de dados, o próximo passo foi desenvolver um script para gerar amostras de dados para popular as tabelas. Nesse caso, utilizei a biblioteca Faker e envolepei todo o código de geração de dados em uma classe Python. Por último, utilizei Pyspark para persistir essas amostras no banco de dados e posteriormente, extrair dados de todas as tabelas, relacionando-as e gerendo uma visão única.

Ademais, foi utilizado a biblioteca Pytest para estar criando rotinas de testes de unidade em cima do código desenvolvido, e também, disponibilizado um arquivo .yml como uma imagem de um container PostgreSQL.


# Como Utilizar
 
1. Clonando repositório para sua máquina
```bash
git clone https://github.com/AironMattos/sicredi_data_engineering_challenge.git
```

2. Abrindo projeto
```bash
cd sicredi_data_engineering_challenge
```

3. Criando e ativando ambiente virtual (via PowerShell)
```bash
python -m venv venv

./venv/Scripts/Activate.ps1
```

4. Instalando pacotes necessários
```bash
pip install -r requirements.txt
```

5. Subindo container docker
```bash
docker-compose up
```

6. Rodando pipelines
```bash
python main.py
```


7. Rodando pipelines
```bash
pytest tests/test.py
```
pytest tests/test.py
