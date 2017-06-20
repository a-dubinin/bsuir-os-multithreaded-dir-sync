/**
 * Программа синхронизации двух каталогах.
 * Разработчик - Дубинин А. В. (http://dubinin.net)
 * 12.02.2017
 */

// Подключение файлов из библиотек
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>

// Объявление символических констант
#define NUM_ARG           3
#define MIN_NUM_THREADS   1
#define BUFFER_SIZE       512
#define ACTION_ERR_CODE   -1
#define D_SEPARATOR       "/"
#define PATH_SELF         "."
#define PATH_PARENT       ".."
#define FILE_NAME_PATTERN "%s%s"
#define DIR_NAME_PATTERN  "%s%s/"

// Объявление символических констант (сообщения пользовательского интерфейса)
#define MSG_INPUT      "Введите количество потоков: "
#define MSG_ERR_ARG    "Ошибка: не указаны каталоги для синхронизации"
#define MSG_ERR_PATH   "Ошибка: некорректное имя каталога '%s' (должно начинаться и заканчиваться символом '/')\n"
#define MSG_ERR_DIR    "Ошибка: невозможно открыть каталог '%s'\n"
#define MSG_ERR_T_NUM  "Ошибка: кол-во потоков должно быть не менее 1"
#define MSG_ERR_THREAD "Ошибка: невозможно открыть поток\n"
#define MSG_ERR_FILE   "Поток %d | Ошибка: невозможно %s файл '%s'\n"
#define MSG_SYNC_FILE  "Поток %d | Скопирован файл '%s' (%d байт)\n"
#define MSG_OPEN       "открыть"
#define MSG_CREATE     "создать"
#define MSG_READ       "прочитать"
#define MSG_WRITE      "записать"
#define MSG_APP_RESULT "Завершено"

// Объявление новой структуры и имени типа - threadData
typedef struct {
    int id;         // идентификатор потока
    char *fileSrc;  // имя файла-источника
    char *fileDest; // имя файла-получателя
} threadData;

/*
 * Прототипы функций syncDirectories, removeDirectory,
 * isDirPath, isValidDirPath, copyFile
 */
void syncDirectories(const char *, const char *, const int);
void removeDirectory(const char *);
int isDirPath(const char *);
int isValidDirPath(const char *);
void *copyFile(void *);

void main(int argc, char *argv[])
{
    // Объявление переменных
    char *dirPath1, *dirPath2; // имена каталогов Dir1 и Dir2
    DIR *dirDest;              // указатель каталога
    int numThreads;            // количество потоков

    // Проверка количества аргументов командной строки
    if (argc < NUM_ARG) {
        puts(MSG_ERR_ARG);
        return;
    }

    // Инициализация имен каталогов аргументами командной строки
    dirPath1 = argv[1];
    dirPath2 = argv[2];

    // Проверка корректности имени каталога Dir1
    if (!isValidDirPath(dirPath1)) {
        printf(MSG_ERR_PATH, dirPath1);
        return;
    }

    /*
     * Проверка корректности имени каталога Dir2
     * и возможности записи в синхронизируемый каталог
     */
    if (!isValidDirPath(dirPath2)) {
        printf(MSG_ERR_PATH, dirPath2);
        return;
    }
    dirDest = opendir(dirPath2);
    if (dirDest == NULL) {
        printf(MSG_ERR_DIR, dirPath2);
        return;
    }
    closedir(dirDest);

    // Диалог ввода пользователем кол-ва потоков (более нуля)
    printf(MSG_INPUT);
    scanf("%d", &numThreads);
    if (numThreads < MIN_NUM_THREADS) {
        puts(MSG_ERR_T_NUM);
        return;
    }

    // Вызов функции синхронизации каталогов
    syncDirectories(dirPath1, dirPath2, numThreads);

    // Вывод на экран сообщения завершения синхронизации
    puts(MSG_APP_RESULT);
    return;
}

/*
 * Функция (рекурсивна) синхронизации двух каталогов.
 *
 * dirSrcPath, dirDestPath - параметры-константы строк
 * (массивы элементов типа char)
 * numThreads - параметр-константа типа int
 */
void syncDirectories(
    const char *dirSrcPath,
    const char *dirDestPath,
    const int numThreads
) {
    // Объявление переменных
    int i, j;                // счетчики циклов
    int numOpenThreads;      // кол-во открытых потоков
    int threadCreateRes;     // результат открытия нового потока
    char *itemSrcName;       // имя очередного элемента-источника
    char *fileDestName;      // имя файла-приемника
    char *innerDirName;      // имя вложенного каталога (в каталоге-источнике)
    char *newDirName;        // имя нового каталога (в каталоге-приемнике)
    DIR *dir1;               // указатель каталога-источника
    struct dirent *dirSrc;   // структура данных каталога-источника
    struct stat itemSrcStat; // структура информации об элементе каталога
    threadData *data = NULL; // данные потока

    // Проверка возможности открытия каталога-источника
    dir1 = opendir(dirSrcPath);
    if (dir1 == NULL) {
        printf(MSG_ERR_DIR, dirSrcPath);
        return;
    }

    pthread_t threads[numThreads]; // инициализация массива потоков
    i = 0;                         // инициализация счетчика потоков

    // Чтение всех элементов каталога-источника в цикле
    while (dirSrc = readdir(dir1)) {
        /*
         * Запись в переменную itemSrcName полного имени элемента-источника,
         * а также в структуру itemSrcStat информации об элементе-источнике.
         */
        asprintf(&itemSrcName, FILE_NAME_PATTERN, dirSrcPath, dirSrc->d_name);
        stat(itemSrcName, &itemSrcStat);

        /*
         * Если элемент-источник является каталогом, то в синхронизируемом
         * каталоге создается новый каталог с таким же названием
         * и рекурсивно вызывается функция syncDirectories() для вложенного
         * и нового каталогов.
         */
        if (S_ISDIR(itemSrcStat.st_mode)
            && (strcmp(dirSrc->d_name, PATH_SELF) != 0)
            && (strcmp(dirSrc->d_name, PATH_PARENT) != 0)
        ) {
            // Запись в переменную newDirName полного названия нового каталога
            asprintf(&newDirName, DIR_NAME_PATTERN, dirDestPath, dirSrc->d_name);
            // Удаление существующего каталога, если он именуется как новый
            removeDirectory(newDirName);
            // Создание нового каталога в синхронизируемом каталоге
            if (mkdir(newDirName, itemSrcStat.st_mode) == 0) {
                /*
                 * Запись в переменную innerDirName
                 * полного названия вложенного каталога
                 */
                asprintf(
                    &innerDirName,
                    DIR_NAME_PATTERN,
                    dirSrcPath,
                    dirSrc->d_name
                );
                /*
                 * Ожидание завершения работы открытых потоков
                 * (для текущего каталога-источника)
                 */
                for (j = 0; j < i; j++) {
                    pthread_join(threads[j], NULL);
                }
                i = 0;
                /*
                 * Рекурсивный вызов функции syncDirectories() для вложенного
                 * каталога (innerDirName) и нового каталога (newDirName)
                 */
                syncDirectories(innerDirName, newDirName, numThreads);
                free(newDirName);
                free(innerDirName);
            }
        }

        /*
         * Если элемент-источник является файлом, то открывается новый
         * поток управления, в котором осуществляется копирование 
         * в каталог-приемник файла с правами доступа файла-источника
         * (функция потока управления copyFile()).
         */
        if (S_ISREG(itemSrcStat.st_mode)) {
            i++;
            // Запись в переменную fileDestName полного имени нового файла
            asprintf(
                &fileDestName,
                FILE_NAME_PATTERN,
                dirDestPath,
                dirSrc->d_name
            );
            // Выделение памяти структуре data (аргумент функции copyFile())
            data = (threadData *)malloc(sizeof(threadData));
            data->id = i;                  // инициализация ID потока
            data->fileSrc = itemSrcName;   // инициализация имени файла-источника
            data->fileDest = fileDestName; // инициализация имени файла-приемника

            // Открытие нового потока управления для копирования файла
            threadCreateRes = pthread_create(
                &threads[i-1],
                NULL,
                copyFile,
                data
            );
            // Вывод сообщения об ошибке в случае невозможности открыть поток
            if (threadCreateRes != 0) {
                puts(MSG_ERR_THREAD);
                i--;
                continue;
            }
        }

        /*
         * Если кол-во открытых потоков становится равным допустимому значению
         * кол-ва потоков, то ожидается завершение работы всех потоков.
         */
        if (i == numThreads) {
            for (j = 0; j < numThreads; j++) {
                pthread_join(threads[j], NULL);
            }
            i = 0;
        }
    }
    numOpenThreads = i; // инициализация кол-ва открытых потоков

    // Ожидание завершения работы открытых потоков
    for (j = 0; j < numOpenThreads; j++) {
        pthread_join(threads[j], NULL);
    }

    // Освобождение памяти
    free(fileDestName);
    free(itemSrcName);
    free(data);
    
    // Закрытие каталога-источника
    closedir(dir1);
    return;
}

/*
 * Функция (рекурсивна) удаления каталога.
 *
 * dirName - параметр-константа строки (массив элементов типа char)
 */
void removeDirectory(const char *dirName)
{
    // Объявление переменных
    DIR *dirPointer;       // указатель каталога
    struct dirent *dirInf; // структура данных каталога
    char *pathBuffer;      // имя элемента каталога
    char *innerDirPath;    // имя вложенного каталога

    // Открытие каталога с проверкой
    dirPointer = opendir(dirName);
    if (dirPointer == NULL) {
        return;
    }

    // Чтение всех элементов каталога-источника в цикле
    while (dirInf = readdir(dirPointer)) {
        /*
         * Если очередной элемент - текущий каталог, либо его родитель,
         * то переход к следующей итерации цикла
         */
        if (strcmp(dirInf->d_name, PATH_SELF) == 0
            || strcmp(dirInf->d_name, PATH_PARENT) == 0
        ) {
            continue;
        }

        // Запись в переменную pathBuffer полного имени элемента
        asprintf(&pathBuffer, FILE_NAME_PATTERN, dirName, dirInf->d_name);
        /*
         * Если элемент является каталогом, то рекурсивно вызывается
         * функция removeDirectory() для него.
         * Иначе, элемент удаляется из каталога.
         */
        if (isDirPath(pathBuffer)) {
            // Запись в переменную innerDirPath полного имени вложенного каталога
            asprintf(&innerDirPath, FILE_NAME_PATTERN, pathBuffer, D_SEPARATOR);
            // Рекурсивный вызов функции removeDirectory()
            removeDirectory(innerDirPath);
            free(innerDirPath); // освобождение памяти
        } else {
            unlink(pathBuffer); // удаление элемента из каталога
        }
        free(pathBuffer);
    }

    closedir(dirPointer); // закрытие потока каталога
    rmdir(dirName);       // удаление каталога
    return;
}

/*
 * Функция проверки типа файла.
 * Определяет, является ли указанный файл каталогом.
 * Возвращает значение типа integer.
 *
 * path - параметр-константа строки (массив элементов типа char)
 */
int isDirPath(const char *path)
{
    struct stat pStat;
    if (stat(path, &pStat)) {
        return 0;
    }
    return S_ISDIR(pStat.st_mode);
}

/*
 * Функция проверки корректности имени каталога (должно начинаться
 * и заканчиваться символом '/').
 * Возвращает значение типа integer.
 *
 * path - параметр-константа строки (массив элементов типа char)
 */
int isValidDirPath(const char *path)
{
    return (path[0] == '/') && (path[strlen(path) - 1] == '/');
}

/*
 * Функция копирования файла (функция потока управления).
 *
 * data - параметр указатель типа void
 */
void *copyFile(void *data)
{
    // Объявление переменных
    char *fileSrcName;            // имя файла-источника
    char *fileDestName;           // имя файла-приемника
    int tid;                      // идентификатор потока
    int fileSrcDescriptor;        // дескриптор файла-источника
    int fileDestDescriptor;       // дескриптор файла-приемника
    struct stat fileSrcStat;      // структура информации о файле-источнике
    char syncBuffer[BUFFER_SIZE]; // буфер данных
    ssize_t readedBytes;          // кол-во считанных байтов из файла-источника

    /*
     * Инициализация идентификатора потока,
     * имен файла-источника и файла-применика
     */
    threadData *td = data;
    tid = td->id;
    fileSrcName = td->fileSrc;
    fileDestName = td->fileDest;

    // Открытие файла-источника на чтение с проверкой
    fileSrcDescriptor = open(fileSrcName, O_RDONLY);
    if (fileSrcDescriptor == ACTION_ERR_CODE) {
        printf(MSG_ERR_FILE, tid, MSG_OPEN, fileSrcName);
        return;
    }
    // Запись в структуру fileSrcStat информации о файле-источнике
    stat(fileSrcName, &fileSrcStat);

    /*
     * Создание нового файла с именем файла-приемника
     * и правами файла-источника, открытие на запись.
     */
    fileDestDescriptor = open(
        fileDestName,
        O_WRONLY|O_CREAT|O_TRUNC,
        fileSrcStat.st_mode
    );

    // Проверка корректности открытия на запись с созданием
    if (fileDestDescriptor == ACTION_ERR_CODE) {
        close(fileSrcDescriptor); // закрытие файла-источника
        printf(MSG_ERR_FILE, tid, MSG_CREATE, fileDestName);
        return;
    }

    // Чтение данных из файла-источника и запись в буфер syncBuffer
    while ((readedBytes = read(fileSrcDescriptor, syncBuffer, BUFFER_SIZE)) > 0) {
        // Запись данных из буфера syncBuffer в файл-приемник
        if (write(fileDestDescriptor, syncBuffer, readedBytes) < readedBytes) {
            // Если запись невозможна - вывод ошибки и выход из цикла
            printf(MSG_ERR_FILE, tid, MSG_WRITE, fileDestName);
            break;
        }
    }
    // Проверка корректности чтения данных из файла-источника
    if (readedBytes == ACTION_ERR_CODE) {
        printf(MSG_ERR_FILE, tid, MSG_READ, fileSrcName);
    }

    // Закрытие файлов
    close(fileSrcDescriptor);
    close(fileDestDescriptor);

    // Вывод на экран результатов копирования
    printf(MSG_SYNC_FILE, tid, fileSrcName, (int) fileSrcStat.st_size);
    return;
}
