const Kafka = require("node-rdkafka");
const fs = require('fs');
const { MongoClient, ServerApiVersion } = require('mongodb');
const uri = "<<yourMongoDBUri>>";


function readConfigFile(fileName) {

    const data = fs.readFileSync("client.properties", "utf8", function (err, data) {
        return data;
    });

    return data.split("\n").reduce((config, line) => {
        const [key, value] = line.split("=");
        if (key && value) {
            config[key] = value.replace('\r', '');
        }
        return config;
    }, {})

}
const config = readConfigFile("client.properties");
config["group.id"] = "node-group";
console.log("start listening");

const consumer = new Kafka.KafkaConsumer(config, { "auto.offset.reset": "earliest" });
consumer.connect();
consumer.on("ready", () => {
    consumer.subscribe(["NandoGoogleDevFest2023ImgAnalysis"]);
    consumer.consume();
}).on("data", message => {
    try {
        let messageFromKafka = JSON.parse(message.value.toString());
        let imageName = messageFromKafka.imgName;
        let imageBase64 = messageFromKafka.imgBase64;
        //cloud vision
        const vision = require('@google-cloud/vision');
        // Creates a client
        const client = new vision.ImageAnnotatorClient();
        var imageBuffer = Buffer.from(imageBase64, 'base64');
        //1. OCR with Text
        client.documentTextDetection(imageBuffer).then(async (data) => {
            const [resultStructure] = data;
            const detections = resultStructure.textAnnotations;
            let textResult = "";
            detections.forEach(text => {
                textResult = textResult + " " + text.description
            });
            const client = new MongoClient(uri, {
                serverApi: {
                    version: ServerApiVersion.v1,
                    strict: true,
                    deprecationErrors: true,
                }
            });

            try {
                // Connect the client to the server	(optional starting in v4.7)
                await client.connect();
                // Send a ping to confirm a successful connection
                let analysisResult = {$set:{
                    "imgName": imageName,
                    "imgResult": textResult,
                    "imgStructure" :resultStructure
                }};
                const dbName = client.db("NandoGoogleDevFest2023DB");
                const query = { imgName: imageName };
                const collectionName = dbName.collection("NandoGoogleDevFest2023ImgAnalysis");
                const options = { upsert: true };
                await collectionName.updateOne(query,analysisResult,options);
            }
            catch (error) {
                console.log(error);
            }
            finally {
                // Ensures that the client will close when you finish/error
                await client.close();
            }
        });
    } catch (error) {
        console.log(error);
    }
});
