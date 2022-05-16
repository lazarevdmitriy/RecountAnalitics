<?php

// --------------------------------------------------------------------------------------------------------------------------------------------
class player { // -------------------------------------------------------------------------------- Класс вывода данных для select -------------
// --------------------------------------------------------------------------------------------------------------------------------------------

  public $options = [
    'name' => '',
    'method' => 'GET',
    'params' => [],
    'type' => '',      // тип записи, для которой вызывается плеер
    'id' => ''         // код записи
  ];

  private $db;
  private $method = 'GET';

  public function __construct($params = []) {
    if ( ! empty($params['route'][0]) ) $this->options['type'] = $params['route'][0];
    if ( ! empty($params['route'][1]) ) $this->options['id'] = $params['route'][1];
    $this->options['route'] = $params['route'];
    $this->options['method'] = ( empty($params['method']) ) ? 'GET' : $params['method'];
    $this->db = new db();
  }

  public function printResult() {

    // Если нет прав отображения, вываливаемся в ошибку
    if ( empty($_SESSION['username']) ) {
      echo (new send( [ 'header' => 403, 'data' => [ 'error' => 1, 'message' => 'error:AccessDenied' ]] ))->printJSON();
      return false;
    }

    $right = "player_".$this->options['type'];
    if ( $this->options['type'] == 'confparams' ) $right = $right = "player_conference";


    if ( ! $this->db->hasRight($right) ) {
      echo (new send( [ 'header' => 403, 'data' => [ 'error' => 1, 'message' => 'error:AccessDenied' ]] ))->printJSON();
      return false;
    }

    switch ( $this->method ) {
      case 'GET': $this->printPlayer(); break;
    }

  }


  //----------------------------------------------------------------------------------------------------------------------------------------------------
  public function getPlayer() { // ---------------------------------------------------------------------------------- отдает массив данных для player --
  //----------------------------------------------------------------------------------------------------------------------------------------------------

    // Выясним, для какого типа нужен плеер
    switch ( $this->options['type'] ) {
      case 'record':         return $this->getRecord(); break;
      case 'conference':     return $this->getConference(true); break;
      case 'confparams':     return $this->getConference(false); break;
      case 'accents':        return $this->accentSynthesis(); break;
      case 'ttsconnector':   return $this->ttsconnectorSynthesis(); break;
      case 'transcription':  return $this->getTranscription(); break;
      case 'journeyfile':    return $this->getFile('journeys'); break;
      case 'recordfile':     return $this->getFile('records'); break;
      default:               return [ 'header' => 404, 'data' => [ 'error' => 1, 'message' => 'error:PageNotFound' ]];
    }

  } //--------------------------------------------------------------------------------------------------------------------------------------- getPlayer --


  //------------------------------------------------------------------------------------------------------------------------------------------------------
  public function printPlayer() { // --------------------------------------------------------------------------------------- выводит JSON данных player --
  //------------------------------------------------------------------------------------------------------------------------------------------------------
    echo (new send( $this->getPlayer() ))->printJSON();
  } //------------------------------------------------------------------------------------------------------------------------------------- printPlayer --


  //------------------------------------------------------------------------------------------------------------------------------------------------------
  public function accentSynthesis() { // -------------------------------------------------------------------------- проигрывает фразу каталога ударений --
  //------------------------------------------------------------------------------------------------------------------------------------------------------
    $text = $this->db->getValue("select correction from accents where id='".$this->options['id']."' limit 1")['result'];
    $this->getSynthesis($text);
  } //--------------------------------------------------------------------------------------------------------------------------------- accentSynthesis --


  //------------------------------------------------------------------------------------------------------------------------------------------------------
  public function ttsconnectorSynthesis() { // --------------------------------------------------------------------- проигрывает фразу теста коннектора --
  //------------------------------------------------------------------------------------------------------------------------------------------------------
    $text = $this->db->getValue("select param5 from connectors where id='".$this->options['id']."' limit 1")['result'];
    $this->getSynthesis($text);
  } //--------------------------------------------------------------------------------------------------------------------------- ttsconnectorSynthesis --


  //------------------------------------------------------------------------------------------------------------------------------------------------------
  private function getSynthesis($text) { // ---------------------------------------------------------------- возвращает синтезированный файл из текста ---
  //------------------------------------------------------------------------------------------------------------------------------------------------------

    /* тут сделать работу с ударениями

    echo "выгружаем ударения"
    echo "phrase=\`echo \$1 | sed \"" > "$astdir/integrations/internal/accents.sh"
    $MySQL -e "select concat('s/',phrase,'/',correction,'/g;\\\') from accents;" $base 2> /dev/null | sed 's/\t//g' | while read line; do
      echo "$line" >> "$astdir/integrations/internal/accents.sh"
    done
    echo "\"\`" >> "$astdir/integrations/internal/accents.sh"
    echo "if [ \"\$3\" == "1" ]; then \
            echo \$phrase | sed \"s/\([А-ЯA-Z]\)/\L\1\$2/g\"; \
          else \
            echo \$phrase | sed \"s/\([А-ЯA-Z]\)/\$2\L\1/g\"; \
          fi; " >> "$astdir/integrations/internal/accents.sh"

          echo "if [ -f $astdir/integrations/internal/accents.sh ]; then \
                  txt=\`$astdir/integrations/internal/accents.sh \"\$1\" \"$accent\" \"$position\" \`; \


    */

    $dataset_id = $this->db->getValue("select dataset_id from users where username='".$_SESSION['username']."'")['result'];
    $dataset = ( empty($dataset_id) ) ? "dataset_id is null" : "dataset_id='${dataset_id}'";

    $cacheDir = '/tmp/synthesisCache';
    $connector = $this->db->getFirstRowArray("
      select id, param as cm, param3 as accent, param4 as position, if(param2='','',concat(' | ',param2)) as pp
      from connectors where type='speechSynthesisConnector'
                        and isActive=1
                        and ${dataset};")['result'];

    if ( ! file_exists($cacheDir) )  mkdir($cacheDir,0777,true);

    $filename = $cacheDir."/".md5(json_encode($connector).$text).".mp3";
    $connector['cm'] = str_replace('$1',$text,$connector['cm']);

    if ( ! file_exists($filename) || filesize($filename) < 100 )
      exec($connector['cm']." ".$connector['pp']." > ${filename}");

    $data = [];
    $data['path'] = $filename;
    $data['name'] = 'synthesis.mp3';

    (new send([ 'header' => 200, 'data' => $data ]))->sendBinary();

  } //--------------------------------------------------------------------------------------------------------------------------------------- getRecord --


  //------------------------------------------------------------------------------------------------------------------------------------------------------
  private function getConference($withFile=true) { // ------------------------------------------------------------------- возвращает запись конференции --
  //------------------------------------------------------------------------------------------------------------------------------------------------------

    $query = "select uploads.path, uploads.name
              from uploads where id in (
                select soundFile from conferences where id='".$this->options['id']."'
              )";

    $data = $this->db->getFirstRowArray($query);
    if (  $data['error'] == 1 ) return [ 'header' => 500, 'data' => $data ];

    $data = $data['result'];

    // Если такой записи нет, выведем ошибку
    if ( ! count($data) ) return [ 'header' => 404, 'data' => [ 'error' => 1, 'message' => 'error:RecordNotFound' ] ];

    // Возмем необходимые пути из конфигурационных файлов
    $config = json_decode(file_get_contents('../../config/paths.conf'),true);

    $filename = $config['uploads']."/".$data['path'];

    if ( ! file_exists("/tmp/voicetech/recordsCache") )
      mkdir("/tmp/voicetech/recordsCache", 0777, true);

    $mp3file = "/tmp/voicetech/recordsCache/".md5($filename).'.mp3';
    if ( ! file_exists($mp3file) )
      exec("lame -a --preset voice \"$filename\" \"$mp3file\"");
    $filename = $mp3file;

    $result['soundFileName'] = basename($filename);
    $result['canDownload'] = $this->db->hasRight('conference_can_download');
    $result['waveform'] = $this->db->getValue("select waveform from conferences where id='".$this->options['id']."'")['result'];

    if ( $result['waveform'] == '' ) {

      if ( ! file_exists("/tmp/voicetech") ) mkdir("/tmp/voicetech/");
      $png = "/tmp/voicetech/".md5_file($filename).".png";
      exec("ffmpeg -y -i \"$filename\" -filter_complex 'aformat=channel_layouts=mono,compand,showwavespic=s=4000x200,crop=in_w:in_h/2:0:0' -c:v png -pix_fmt monob -frames:v 1 ${png}");

      $waveform = '';
      if ( file_exists($png) && function_exists('imagecreatefrompng') ) {
        $a = imagecreatefrompng($png); $i = 0; $h = 100;
        while ( $i < 4000 ) {
          $c = 0; $arr[$i] = 0;
          while ( $c < $h ) {
            if ( imagecolorat($a, $i, $c ) == 1 ) {
              $arr[$i] =  (100-$c)/100;
              break;
            }
            $c++;
          }
          $i++;
        }
        // Нормализуем до максимальной высоты в 70%
        $k = 0.7/max($arr);
        for ( $i=0; $i<count($arr); $i++ ) $arr[$i] = round($arr[$i] * $k, 4);

        $waveform = json_encode($arr);
        $result['waveform'] = $waveform;
        $this->db->getResult("UPDATE conferences SET waveform='${waveform}' WHERE id='".$this->options['id']."'");

      }

    }

    if ( $withFile ) {
      $handle = fopen($filename, "rb");
      $file = fread($handle, filesize($filename));
      fclose($handle);
      $result['soundFile'] = base64_encode($file);
    }

    return [ 'header' => 200, 'data' => $result ];

  } //--------------------------------------------------------------------------------------------------------------------------------------- getRecord --



  //------------------------------------------------------------------------------------------------------------------------------------------------------
  private function getRecord() { // -------------------------------------------------------------------------------------- возвращает JSON данных record --
  //------------------------------------------------------------------------------------------------------------------------------------------------------

    $query = "select soundFile, id, DATE_FORMAT(calldate,concat(opt('dateFormat'),' ',opt('timeFormat'))) as `date`, isRecorded
              from phone_cdr where interactionID='".$this->options['id']."' and soundFile!='' limit 1";

    $data = $this->db->getFirstRowArray($query);
    if (  $data['error'] == 1 ) return [ 'header' => 500, 'data' => $data ];
    $data = $data['result'];

    // Если такой записи нет, выведем ошибку
    //if ( ! count($data) ) return [ 'header' => 404, 'data' => [ 'error' => 1, 'message' => 'error:RecordNotFound' ] ];

    //if ( $data['isRecorded'] == 0 ) return [ 'header' => 404, 'data' => [ 'error' => 1, 'message' => 'error:EmptySoundFile' ] ];

    // Возмем необходимые пути из конфигурационных файлов
    $config = file_get_contents('../../config/paths.conf');

    $config = json_decode($config,true);

    $paths = [];

    if ( ! empty($config['records']) ) $paths[] = $config['records'];
    if ( ! empty($config['recordsArchive']) ) $paths[] = $config['recordsArchive'];

    $sound = $this->getSoundFile($paths,$data['soundFile']);

    if ( $sound === false ) return [ 'header' => 404, 'data' => [ "error" => 1, "message" => "error:SoundFileMissing" ] ];
    if ( $sound === true  ) return [ 'header' => 405, 'data' => [ "error" => 1, "message" => "error:SoundFileIsProcessing" ] ];

    $data['soundFileName'] = $sound['name'];
    $data['canDownload'] = $this->db->hasRight('player_can_download');
    $data['soundFile'] = $sound['binary'];

    return [ 'header' => 200, 'data' => $data ];

  } //--------------------------------------------------------------------------------------------------------------------------------------- getRecord --


  //------------------------------------------------------------------------------------------------------------------------------------------------------
  private function getTranscription() { // ----------------------------------------------------------------------- возвращает JSON данных transcription --
  //------------------------------------------------------------------------------------------------------------------------------------------------------

    /*
    $data = $this->getRecord();

    // Если ошибка получения записи, вываливаемся из функции
    if ( $data['header'] != 200 ) return $data;

    // Откинем header
    $data = $data['data'];*/

    // Берем данные о звонке


    $query = "select * from (
      select id, interactionID, DATE_FORMAT(calldate, concat(opt('dateFormat'),' ',opt('timeFormat'))) AS `date`,
      RIGHT(IF((caller <> ''),caller,src),10) AS `caller`,
      RIGHT(IF((callee <> ''),callee,dst),10) AS `callee`,
      IF(isManager = src, RIGHT(caller, 10), RIGHT(callee, 10)) AS `isManager`,
      calldate AS `dt`,
      '' AS `callerPic`,
      '' AS `calleePic`,
      'voice' AS `src`
from phone_cdr
      where lastapp <> 'Hangup'
        and interactionID = '".$this->options['id']."'
             union
      select chats.id, chats.interactionID, DATE_FORMAT(chats.dtStart, concat(opt('dateFormat'),' ',opt('timeFormat'))) AS `date`,
             chats.customer AS `caller`,
             ifnull(concat(users.surname,' ',users.firstname),'') AS `callee`,
             ifnull(concat(users.surname,' ',users.firstname),'') AS `isManager`,
             chats.dtStart AS `dt`,
             '' AS `callerPic`,
             concat('/uploads/', users.upload_id) AS `calleePic`,
             'chat' AS `src`
      from chats
      left join users on users.user_id = chats.user_id
      where chats.interactionID = '".$this->options['id']."'
      order by dt desc) as intrs limit 1";

    $trans = $this->db->getFirstRowArray($query);

    if (  $trans['error'] == 1 ) return [ 'header' => 500, 'data' => $trans ];
    $trans = $trans['result'];

    // Добавим данные по абонентам в итоговый массив
    //$data = array_merge($data,$trans);
    $data = $trans;

    if ( $trans['src'] == 'voice' ) {
      $query = "select id,
                       right(speaker,10) as speaker,
                       start,
                       (start+duration) as end,
                       duration,
                       if(commited_voice='','".$this->db->dictionary("SpeechNotRecognized")."',commited_voice) as voice,
                       'phone_cdr_data' as `table`
                from phone_cdr_data where interactionID='".$data['interactionID']."'
                order by start,end,speaker";
    } else {
      $query = "select chats_data.id as id,
                       ifnull(chats_data.customer,'".$trans['callee']."') as speaker,
                       TIME_TO_SEC( TIMEDIFF(chats_data.dt,chats.dtStart) ) as start,
                       TIME_TO_SEC( TIMEDIFF(chats_data.dt,chats.dtStart) ) as end,
                       0 as duration,
                       message as voice,
                       'text_journeys' as `table`
                from chats_data
                left join chats on chats.id = chats_data.chat_id
                where chats.interactionID='".$data['interactionID']."'
                order by start,end,speaker";
    }
    $data['dialog'] = $this->db->getAllDataArray($query)['result'];
    if ( empty($data['dialog']) ) $data['dialog'] = false;


    $dataset_id = $this->db->getValue("select dataset_id from users where username='".$_SESSION['username']."'")['result'];
    $datasetWhere = ( empty($dataset_id) ) ? "dataset_id is null" : "dataset_id='${dataset_id}'";

    $query = file_get_contents('../classes/views/list_interaction_tags.sql');
    $query = str_replace("{dataset}",$datasetWhere,$query);
    $query = str_replace("{interactionID}",$data['interactionID'],$query);

    $data['tags'] = $this->db->getAllDataArray($query)['result'];

    if ( empty($data['tags']) ) $data['tags'] = false;
    else {
  
      for ( $i=0; $i<count($data['tags']);$i++ ){
         $words = $data['tags'][$i]['words'];
          $data['tags'][$i]['words'] = [];
      	  for ( $j=0; $j<count($words);$j++ ) {
      	    $data['tags'][$i]['words'][$j] = ['word'=> $words, 'count'=> $data['tags'][$i]['count'], 'search_channel'=> $data['tags'][$i]['search_channel']]; 
      	  
      	 }
      }
        
    }
    // выведем список возможных тэгов для добавления синонима, если у пользователя есть права
    $data['taglist'] = false;
    if ( $this->db->hasRight("add_change_phone_cdr_tag_candidates") ) {
      $data['taglist'] = $this->db->getAllDataArray("
        select id, name, concat('color',emotion) as color
        from phone_cdr_tags where deleted != 1 and ${datasetWhere}");
      $data['taglist'] = ( ! $data['taglist']['error'] ) ? $data['taglist']['result'] : false;
    }

    // Если есть права доступа на изменение текстов - выведем шаблоны
    $data['templates'] = [];

    if ( $this->db->hasRight("add_change_phone_cdr_data") )
      $data['templates'][] = [ 'table' => 'phone_cdr_data', 'template' => ( new record([ 'table' => 'phone_cdr_data' ]))->getTemplate() ];
    if ( $this->db->hasRight("add_change_text_journeys") )
      $data['templates'][] = [ 'table' => 'text_journeys', 'template' => ( new record([ 'table' => 'text_journeys' ]))->getTemplate() ];

    return [ 'header' => 200, 'data' => $data ];

  } //-------------------------------------------------------------------------------------------------------------------------------- getTranscription --


  //------------------------------------------------------------------------------------------------------------------------------------------------------
  private function getFile($type) { // ----------------------------------------------------------------------- возвращает звуковой файл указанного типа --
  //------------------------------------------------------------------------------------------------------------------------------------------------------

    $route = $this->options['route'];
    array_shift($route);
    $filename = implode('/',$route);

    if ( $type == 'records' )
      $filename = $this->db->getValue("select soundFile from phone_cdr where interactionID='${filename}' and soundFile!='' limit 1")['result'];

    // Если файл пустой
    if ( empty($filename) ) {
      (new send([ 'header' => 404 ]))->sendError();
      return;
    }

    // Возьмем необходимые пути из конфигурационных файлов
    $config = file_get_contents('../../config/paths.conf');
    $config = json_decode($config,true);

    $paths = [];

    if ( ! empty($config[$type]) ) $paths[] = $config[$type];
    if ( ! empty($config[$type.'Archive']) ) $paths[] = $config[$type.'Archive'];

    $data = [];
    $data['path'] = $this->getSoundFile($paths,$filename,true);

    $data['name'] = explode('/',$data['path']);
    $data['name'] = $data['name'][count($data['name'])-1];

    (new send([ 'header' => 200, 'data' => $data ]))->sendBinary();

  } //----------------------------------------------------------------------------------------------------------------------------------------- getFile --

  //------------------------------------------------------------------------------------------------------------------------------------------------------
  public function getSoundFile($paths,$file,$onlyPath=false,$extensions = [ 'wav', 'mp3', 'wav.gz' ]) { // ---------------------- возвращает звуковой файл записи --
  //------------------------------------------------------------------------------------------------------------------------------------------------------
  // На входе принимает массивы возможных путей нахождения файла и его расширения
  //------------------------------------------------------------------------------------------------------------------------------------------------------

    $filename = '';
    for ( $i=0;$i<count($paths); $i++ )
      for ( $j=0;$j<count($extensions); $j++ ) {
        error_log($paths[$i] . "/${file}." . $extensions[$j]);
        if (file_exists($paths[$i] . "/${file}." . $extensions[$j]))
          $filename = $paths[$i] . "/${file}." . $extensions[$j];
      }
    if ( empty($filename) )
      for ( $i=0;$i<count($paths); $i++ )
        for ( $j=0;$j<count($extensions); $j++ )
          if (file_exists($paths[$i] . "/${file}-in." . $extensions[$j]))
            $filename = $paths[$i] . "/${file}-in." . $extensions[$j];

    // Если файл не найден, возьмем заглушку
    if ( empty($filename) ) $filename = '/opt/voicetech/web/wwwroot/assets/sounds/soundFileNotFound.wav';

    // Если идет обработка возвращаем true
    if ( strpos($filename,'-in.') ) return true;

    // Если файл пустой, возвращаем false
    if ( filesize($filename) < 50 ) return false;

    // Если уже есть файл в кэше
    $mp3file = "/tmp/voicetech/recordsCache/".md5($filename).'.wav';
    if ( ! file_exists("/tmp/voicetech/recordsCache") )
      mkdir("/tmp/voicetech/recordsCache", 0777, true);
    if ( file_exists($mp3file) ) {
      $filename = $mp3file;

    // Если мы имеет дело с gz архивом записи
    } else if ( strpos($filename,'.wav.gz') !== false ) {
      exec("cp \"${filename}\" \"${mp3file}.temp.gz\"");
      exec("gunzip \"${mp3file}.temp.gz\"");
      exec("lame \"${mp3file}.temp\" \"${mp3file}\"");
      if ( ! file_exists($mp3file) ) {
        exec("mv \"${mp3file}.temp\" \"${mp3file}\"");
      } else {
        unlink("${mp3file}.temp");
      }
      $filename = $mp3file;

     // Если wav-файл очень большой, пережимаем его перед отдачей
    } else if ( filesize($filename) > 20971520 && strpos($filename,'.wav') !== false ) {
      exec("lame \"${filename}\" \"${mp3file}\"");
      $filename = $mp3file;
    }

    if ( $onlyPath ) return $filename;

    // Открываем файл
    $handle = fopen($filename, "rb");
    $file = fread($handle, filesize($filename));
    fclose($handle);

    // Возвращаем файл в кодировке base64
    return [ 'name' => basename($filename), 'binary' => base64_encode($file) ];

  } //------------------------------------------------------------------------------------------------------------------------------------ getSoundFile --
}

