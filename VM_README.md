# 수업 VM 전달용 최소 저장소

CentOS 6.6 / Python 2.6.6 / Java 7 / Spark 1.3.0 / CDH 5.4.3용이다.
1 core·RAM 3 GB·추가 HDD 100 GB 환경에서 원본부터 월별 지표까지 사이트별로 계산한다.

## 포함 범위

- `vm/legacy/`: 실행 코드·SQL·상세 안내
- `outputs/network/download_manifest.json`: 원본 이름·크기·SHA1
- `outputs/network/classification_freeze.json`: 동결 분류와 관측 조건
- `outputs/network/monthly_metrics_audit.json`: 362사이트·22,806행 검산 기준
- `sql/site_metrics_monthly_schema.sql`: 7열 서빙 테이블 DDL 초안
- `tests/test_legacy_vm.py`: 현대 Python 호스트에서 실행하는 파서·비교기 테스트

JSON 3개는 실행 의존성이므로 포함한다. 원본 7z·XML·Parquet, `.venv`, 나머지 분석 결과와 개인 문서는 커밋하지 않는다. `.gitignore`는 허용 목록 방식이다. 파일을 새로 추가할 때는 목록도 갱신한다. `.gitattributes`는 JSON 바이트를 보존한다.

## VM으로 전달

원격 저장소가 없어도 호스트에서 만든 `teamproject-vm.bundle`을 공유 폴더나 파일 복사로 VM에 전달할 수 있다. VM에서 `git --version`으로 Git 설치 여부를 확인한다.

```bash
git clone /실제/전달경로/teamproject-vm.bundle ~/teamproject
cd ~/teamproject
git log -1 --oneline
```

bundle에는 커밋된 파일과 Git 이력이 들어 있다. 원본 덤프는 별도로 공유 폴더에 준비한다. bundle로 clone한 저장소의 origin은 bundle 파일 경로다. 나중에 원격 저장소를 쓰려면 해당 URL로 origin을 변경한다.

Git이 없는 VM에는 호스트에서 다음처럼 ZIP으로 내보내 전달할 수도 있다. ZIP에는 파일만 들어가며 Git 이력은 포함하지 않는다.

```powershell
git archive --format=zip --output=teamproject-vm.zip HEAD
```

## 실행 준비

7z/p7zip-plugins 설치, 추가 디스크 마운트와 HDFS DataNode 저장 위치를 먼저 확인한다. 원본은 manifest 크기·SHA1과 일치해야 한다. 현대 Python requirements를 VM에 설치할 필요가 없다.

아래 경로를 실제 VM 경로로 바꾼다. `/data/work`에는 현재 사용자의 쓰기 권한, HDFS `/user/cloudera`에는 생성 권한이 필요하다.

```bash
python vm/legacy/run_site.py \
  --project "$PWD" \
  --archives /실제/공유폴더/stackexchange_20240402_bis \
  --site homebrew.stackexchange.com \
  --work /data/work \
  --hdfs-run /user/cloudera/r3-vm-trial-001
```

자세한 실패 처리·공간 점검·결과 확인은 [실행 안내](vm/legacy/README.md)를 따른다. 동일한 run 경로를 덮어쓰지 않는다.

## 검증 상태

호스트의 파서·비교기 3개 테스트 및 기존 전체 작업 폴더에서의 Spark SQL 테스트 5개가 통과했다. 최소 저장소에는 현대 Spark 분석 전체 의존성을 넣지 않았으므로 Spark 테스트 묶음은 포함하지 않았다.

이 저장소 자체의 호스트 테스트는 현대 Python에서 다음으로 실행한다. Python 2.6 VM용 테스트 명령이 아니다.

```bash
python -m unittest discover -s tests -p test_legacy_vm.py -v
```

실제 Python 2.6·Spark 1.3 VM 실행과 Hive/Impala 조회는 아직 미검증이다. 작은 사이트의 `validated.json`을 확인한 뒤 범위를 확대한다.
