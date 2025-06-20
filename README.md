# basegis

open WSL
laptop: cd /mnt/c/Users/mksch/Desktop/Personal/GitHub/basegis
GEO: cd /mnt/c/Users/mschappert/Desktop/GitHub_mik/basegis
GEO new: cd /mnt/d/Mikayla_RA/RA_S25/basegis

type docker to make sure its running

make sure it sees everything/all files : ls
docker build -t basegis .




cd /mnt/c/Users/mschappert/Desktop/GitHub_mik/basegis/ 
docker run -p 8888:8888 -p 8787:8787 -v $(pwd):/home/gisuser/data/ -it basegis

D:\Mikayla_RA\RA_S25\Time_Series




docker run -it -p 8888:8888 -p 8787:8787 \
  -v /mnt/c/Users/mschappert/Desktop/GitHub_mik/basegis:/app \
  -v /mnt/d/Mikayla_RA/RA_S25/Time_Series:/data \
  basegis


cd /mnt/d/Mikayla_RA/RA_S25/basegis/

docker run -p 8888:8888 -p 8787:8787 -v $(pwd):/home/gisuser/code/ -it basegis
