pipeline {
    agent any

    environment {
        // La credencial de Kaggle se inyecta desde Jenkins Credentials (Secret file o Username/Password)
        KAGGLE_CONFIG_DIR = "${WORKSPACE}/.kaggle_creds"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Descarga de Datos (Kaggle API)') {
            steps {
                // Inyecta el kaggle.json sin exponerlo en el repositorio
                withCredentials([file(credentialsId: 'kaggle-json-secret', variable: 'KAGGLE_JSON_FILE')]) {
                    sh '''
                        mkdir -p .kaggle_creds
                        cp "$KAGGLE_JSON_FILE" .kaggle_creds/kaggle.json
                        chmod 600 .kaggle_creds/kaggle.json
                        python3 -m pip install kaggle
                        python3 src/ingestion/download_data.py
                    '''
                }
            }
        }

        stage('Build & Tests') {
            steps {
                sh '''
                    pytest tests/
                '''
            }
        }

        stage('Deploy') {
            steps {
                sh '''
                    docker compose up -d --build
                '''
            }
        }
    }
}
