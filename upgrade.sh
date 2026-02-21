git fetch --all
git rebase fork/main
docker compose down 
docker compose -f docker-compose-lorne.yml build --no-cache
docker compose -f docker-compose-lorne.yml up -d
