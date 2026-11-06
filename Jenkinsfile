pipeline {
    agent any

    environment {
        KAGGLE_CREDS = credentials('kaggle-credentials')
        PYTHONPATH = "${WORKSPACE}"
    }

    stages {
        stage('Checkout & Lint') {
            steps {
                echo "Verificando integridad del repositorio y commit desplegado..."
                sh 'git rev-parse --short HEAD'
            }
        }

        stage('Pruebas Automatizadas (Pytest)') {
            steps {
                echo "Ejecutando pruebas unitarias, de integracion y formula de cierre..."
                sh '''
                    python3 -m pip install --upgrade pip
                    python3 -m pip install -r requirements-api.txt pytest
                    python3 -m pytest -v tests/
                '''
            }
        }

        stage('Construccion y Despliegue Docker') {
            steps {
                echo "Construyendo contenedores e iniciando servicios..."
                sh 'docker compose up -d --build api'
            }
        }

        stage('Smoke Test API') {
            steps {
                echo "Validando disponibilidad de endpoints desplegados..."
                sleep 5
                sh 'curl -f http://api:5000/health || curl -f http://localhost:5000/health'
            }
        }
    }

    post {
        failure {
            echo "El pipeline ha fallado en las pruebas. Despliegue abortado para proteger produccion."
        }
        success {
            echo "Pipeline exitoso: Servicios desplegados y verificados."
        }
    }
}
