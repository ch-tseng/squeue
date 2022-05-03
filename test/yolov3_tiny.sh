/home/chtseng/frameworks/darknet/darknet detector train \
    /WORKING/modelSale/Car_Plate_detect/aug_20220425/cfg_train/obj.data \
    /WORKING/modelSale/Car_Plate_detect/aug_20220425/cfg_train/yolov3-tiny.cfg \
    /home/chtseng/tools/Make_YOLO_Train/pretrained/yolov3-tiny.conv.15 \
    -dont_show \
    -mjpeg_port 8090 \
    -clear \
    -gpus 0
