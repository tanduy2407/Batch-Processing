from etl_staging import main_staging
from etl_production import main_production

if __name__ == '__main__':
    main_staging()
    main_production()