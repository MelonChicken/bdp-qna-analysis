# 수업 VM용 이식 실행 파일

대상: CentOS 6.6, Python 2.6.6, Java 7, Spark 1.3.0, Hadoop 2.6/CDH 5.4.3.
CPU 1 core, RAM 약 3 GB, 추가 HDD 100 GB를 기준으로 **한 사이트씩 실행**한다.
기본 이미지는 7z가 없으므로 먼저 p7zip/p7zip-plugins를 설치한다.

현재 상태: 파서·실행 래퍼·Scala 실행기·SQL을 구현했다. 호스트의 Python 및 현대 Spark에서 검증할 수 있는 부분과 실제 구형 VM 검증은 구분한다. Spark 1.3/Java 7 VM에서의 컴파일·실행 및 Hive/Impala 조회는 아직 확인하지 않았다.

2026-10-02 호스트 검증: Python 파서·비교기 3개 테스트, Spark 지표·이식 SQL 5개 테스트 통과. Python 2.6 인터프리터 실행과 Scala 2.10/Spark 1.3 컴파일은 이 호스트에서 검증하지 못했다.

## 실행 흐름

`7z 스트림 → Python 2.6 TSV → HDFS → Spark 1.3 Parquet → 월별 SQL → 기준 결과 대조`

- `prepare.py`: manifest·동결 해시·원본 크기/SHA1 검사, 실제 XML member 대소문자 확인, UTC 변환, NULL 보존, 10만 행 단위 TSV. XML 전체나 전체 행을 메모리에 보관하지 않는다.
- `run.scala`: Spark 1.3의 HiveContext·createDataFrame·registerTempTable·saveAsParquetFile 사용. 테이블별 year 파티션을 작성하고 행 수·타입·중복 ID를 검증한 뒤 지표를 계산한다.
- `metrics.sql`: 7일 경계·연말 답변·무응답·NULL 규칙을 유지한 별도 SQL. 달력은 Python이 63행으로 생성한다.
- `run_site.py`: 사전 점검, HDFS 전송, `local[1]` 실행, 로그, 자체 성공 마커 검사, 63개월 전체 필드 대조와 실행 영수증을 담당한다.

현재 원본 7z 입력 경로를 구현했다. 기존 Parquet만 입력받는 재사용 모드는 아직 제공하지 않는다. Python 3 패키지 설치는 필요하지 않다. Scala 실행기는 VM의 spark-shell로 읽으며 별도 sbt 설치도 요구하지 않는다. API 선택 기준은 [Spark 1.3 공식 문서](https://archive.apache.org/dist/spark/docs/1.3.0/sql-programming-guide.html)다.

## 1. 입력과 디스크 준비

프로젝트에서 `vm/legacy/`와 다음 세 파일을 VM으로 전달한다. 기존 월별 결과는 **검산용**이며 VM 계산 입력으로 사용하지 않는다.

```text
outputs/network/download_manifest.json
outputs/network/classification_freeze.json
outputs/network/monthly_metrics_audit.json
```

원본 `.7z`는 호스트 공유 폴더에서 읽는다. `stackexchange_20240402_bis` 다운로드라도 위 manifest의 크기/SHA1과 같아야 한다. 불일치하면 실행을 중단한다.

추가 HDD를 `/data`에 마운트한 경우 다음을 확인한다. 아직 마운트되지 않았다면 먼저 디스크 준비를 완료한다. 현재 코드가 디스크를 포맷하거나 HDFS 저장 위치를 변경하지 않는다.

```bash
df -h / /data
mount | grep ' /data '
hdfs getconf -confKey dfs.datanode.data.dir
hdfs dfsadmin -report
which 7z
rpm -q p7zip p7zip-plugins
```

OS 작업 폴더와 HDFS block 저장소가 모두 추가 디스크를 사용하는지 확인한다. 루트 디스크 여유 8.1 GB만으로 전수를 계획하지 않는다. `/data/work`에는 실행 사용자의 쓰기 권한이 필요하다. HDFS의 `/user/<계정>`도 쓰기 가능해야 한다.

입력 TSV는 로컬과 HDFS에 한 번씩 존재하며 Parquet·Spark 임시 공간이 추가로 필요하다. 100 GB가 모든 사이트의 중간 파일을 동시에 보관할 충분한 공간이라는 보장은 없다. 처음에는 작은 사이트로 측정한다. 로컬 여유 2 GiB 미만이면 시작을 막고, TSV 작성 중에는 1 GiB 예비 공간을 검사한다. 이는 전체 처리량에 대한 용량 보장이 아니다. HDFS 여유 공간은 별도로 모니터링한다.

## 2. 작은 사이트 하나 실행

다음 경로는 예시다. 프로젝트·공유 폴더·HDFS 사용자 이름을 실제 경로로 바꾼다. 프로젝트 루트에서 실행한다.

```bash
python vm/legacy/run_site.py \
  --project /home/cloudera/teamproject \
  --archives /mnt/hgfs/StackExchange_2024Q1/stackexchange_20240402_bis \
  --site homebrew.stackexchange.com \
  --work /data/work \
  --hdfs-run /user/cloudera/r3-vm-trial-001
```

실제 공유 폴더 경로는 VM에서 확인한다. 기본 driver 메모리는 `768m`, shuffle 파티션은 2다. 3 GB 전체를 Spark에 할당하지 않는다. 필요하면 측정 후 `--driver-memory`를 지정한다. 작업 중 다른 대용량 계산을 동시에 실행하지 않는다.

같은 로컬/HDFS 결과가 존재하면 덮어쓰거나 재사용하지 않고 중단한다. 실패 시 부분 결과와 로그를 보존하므로 재실행은 새 run 이름을 사용한다. 자동 삭제·전수 무인 반복·실패 상태의 자동 재개는 구현하지 않았다.

## 3. 결과 확인

로컬 작업 폴더는 `<work>/<run 이름>-<site>/`다.

|산출물|의미|
|prepared.json|세 XML 스트림과 7z 정상 종료, TSV 준비 완료|
|spark.log|Scala 컴파일·Spark 실행 로그|
|audit.json|VM이 새로 계산한 63개월 결과|
|validated.json|기준과 비교 통과, 입력·코드·분류 해시 및 완료 시각|

HDFS 결과는 `<hdfs-run>/<site>/output/`다. `raw/<table>/site=<site>/year=<year>`에 원본 테이블 Parquet, `audit/`에 JSON 행, `serving/`에 7열 월별 Parquet이 생긴다. 각 사이트 실행은 독립된 결과 트리다. 이를 전체 공통 테이블로 게시하는 작업은 별도로 수행한다.

Spark 자체 `_SUCCESS`나 shell의 종료 코드만으로 성공을 판단하지 않는다. 실행기가 작성한 `_VM_SUCCESS`와 로컬 `validated.json`을 함께 확인한다. Scala 구문 오류로 shell이 0을 반환해도 별도 마커가 없으면 래퍼가 실패한다.

건수·분류·NULL 위치·감사 분자/분모는 정확히 비교하고 비율은 절대 오차 1e-12를 적용한다. JSON에서 생략된 NULL 필드는 NULL로 해석한다. 7열 서빙 Parquet을 Hive/Impala에서도 직접 읽는 엔진 간 검증은 별도다.

## 4. 범위 확대

작은 사이트 → 본/meta 쌍 → 기존 파일럿 → 362사이트 순서로 확대한다. 각 사이트가 통과하면 필요 자료를 보존하고 중간 TSV 공간 회수 계획을 세운 뒤 다음 사이트를 실행한다. 자동 삭제 명령은 제공하지 않는다. 전체 완료는 사이트별 validated 기록이 362개이고 전체 키 중복 없이 22,806행인지 확인한 후 선언한다.

월별 지표 계산과 SciPy 통계검정은 별도다. 이번 파일은 VM에서 월별 지표까지 계산한다. Python 2.6용 통계검정 이식은 포함하지 않는다.

## 호스트 검증

```bash
python -m unittest discover -s tests -p test_legacy_vm.py -v
```

현대 Python 호스트에서 실행하며 스트리밍·UTC·NULL·분할·실패 입력·결과 대조를 검사한다. Python 2.6 VM에서 실행할 테스트가 아니다. 원래 전체 작업 폴더의 `test_spark_metrics.py`에서는 7일·연말·교차 사이트·무응답 경계의 SQL 동일성도 검사했지만, 그 파일과 현대 Spark 의존성은 최소 전달 저장소에 포함하지 않는다. 현대 Spark에서 SQL이 통과해도 Spark 1.3 런타임 호환성이 확인된 것은 아니다.
