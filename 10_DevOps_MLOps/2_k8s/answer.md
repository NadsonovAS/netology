1.	Ответить на вопросы:

●	Что такое k8s?
    Кратко - система оркестрации контейнеров для автоматизации развертывания, масштабирования и управления контейнеризованными приложениями

●	В чём преимущество контейнеризации над виртуализацией?
    1 - Контейнеры шарят ядро хоста и занимают меньше ресурсов, чем полноценные виртуальные машины
    2 - Контейнеры запускаются и останавливаются за гораздо меньшее время
    3 - Образ контейнера позволяет гарантировать одинаковую работу на любом хосте

●	В чём состоит принцип самоконтроля k8s?
    Контроль состояния, автоматические развертывания и откаты реплик контейнеров внутри узлов кластера

●	Как вы думаете, зачем Вам понимать принципы деплоя в k8s?
    Вопрос риторический, как по мне. Но если отвечать, то для без понимания CI/CD будет проблематично складно организовать свою работу, в частности выкатывать новые вещи в прод.

●	Какое из средств управления секретами наиболее распространено в использовании совместно с k8s?
    Я ж вкатун и еще не хожу на конференции, откуда мне знать)
    Но беглый google говорит, что в крупных компаниях (банки, облака, финтех) - HashiCorp Vault.

●	Какие типы нод есть в k8s, каковы их базовые функции?
    Мастер нода - содержит компоненты управления кластером: API server, Scheduler, Controller Manager...
    Отвечает за принятие решений и контроль состояния кластера.

    Воркер нода - исполняет поды и контейнеры приложений


2.   Написать манифест, который будет содержать в себе следующие условия:
    yaml файлик лежит рядом с этим md файлом.
    

В orbstack на macOS данный yaml поднимается:

kubectl apply -f netology-ml-deployment.yaml
deployment.apps/netology-ml created

kubectl get deployments
kubectl get pods

NAME          READY   UP-TO-DATE   AVAILABLE   AGE
netology-ml   2/2     2            2           33s
NAME                           READY   STATUS    RESTARTS   AGE
netology-ml-6d45dbb995-5bjbm   1/1     Running   0          33s
netology-ml-6d45dbb995-vm5r4   1/1     Running   0          33s