# Oracle to Informix Migration

오라클 DB를 Informix로  변환할 일이 있어 Schema 변환을 위한 python 스크립트를 작성해 보았습니다.

## 오라클 라이브러리
Python 접속을 위해 python-oracledb 라이브러리 설치가 필요하다는 군요.
python-oracledb 설치는 아래와 같이 하면 됩니다만, python3.10 이상 버전이 필요합니다.
pip install oracledb

## Oracle Connection
접속정보는 아래와 같이 oracle.cfg파일에 명시해 줍니다.

```
[CONFIG]
CONNECT_STRING = b1-oracle:1521/XE
TYPE_CONV_FORCE_TABLE = column_conv_table.csv
ORACLE_USER = oracle
ORACLE_PASSWORD = passw0rd
```

## 컬럼타입 변환
DBMS에서 사용하는 타입들은 다소 차이가 있지만, 변환가능한 타입은 cnv_ 함수로 만들었습니다.
강제로 변환이 필요한 경우 column_conv_table.csv 파일에 컬럼 정보를 추가하시면 강제로 그 값을 사용합니다.

## Partition 변환 
Oracle의 Partition과 Informix Fragment는 List를 제외하면 거의 호환되지 않으므로 무시하시기 바랍니다.

## 수행시
수행시 사용자명을 지정하여야 합니다.
python cnv_oracle_schema.py -u HR
