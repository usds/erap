FROM jupyter/scipy-notebook

RUN conda install --quiet --yes \
    'sqlalchemy' \
    'psycopg2' \
    'ipython-sql' \
    'openpyxl' \
    'pyarrow' \
    'cenpy' \
    'geopandas'

RUN /opt/conda/bin/pip install cpi
