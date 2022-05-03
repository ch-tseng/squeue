/home/chtseng/frameworks/darknet/darknet detector train \
    /WORKING/modelSale/Car_Plate_detect/aug_20220425/cfg_train/obj.data \
    /WORKING/modelSale/Car_Plate_detect/aug_20220425/cfg_train/yolov4-tiny.cfg \
    /home/chtseng/tools/Make_YOLO_Train/pretrained/yolov4/yolov4-tiny.conv.29 \
    -dont_show \
    -mjpeg_port 8090 \
    -clear \
    -gpus 0
