# L00 — Instructor notes

* **Assign as homework** one week before L01; downloads (images ≈ 5–8 GB for L01–L06, dataset 50 MB)
  do not work well on classroom Wi-Fi. Ask students to paste the last lines of `check_env.sh` in the LMS.
* Timing in class if needed: install check 10 min, resources 5 min, hello-world 5 min, datasets 10 min,
  check_env 10 min, Q&A 10 min.
* Most common problems: Docker Desktop not started; memory left at the 2 GB default; Windows users
  cloning under `C:\` instead of WSL; corporate proxies blocking `d37ci6vzurychx.cloudfront.net`
  (provide the taxi file on a USB stick / LMS as fallback); disk full (old images from other courses).
* Docker Desktop memory: labs L01–L06 were verified with a 4 GB Docker VM on an 8 GB Apple Silicon Mac;
  recommend 6 GB for everything after L06.
* Disk budget (images, approximate unpacked sizes): `de-labs/hadoop` 2.2 GB, `de-labs/sqoop` +0.1 GB,
  `apache/hive:4.2.1` 3.9 GB, `spark:4.1.3-python3` 2.2 GB, `postgres:18.6` 0.7 GB.
* Grading: completion only (screenshot/paste of `READY` from `check_env.sh` + `hello-world` output),
  plus the 5 checkpoint answers.
