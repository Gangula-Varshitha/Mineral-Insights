const mongoose = require('mongoose');
const csvtojson = require('csvtojson');

// MongoDB Connection String
const MONGO_URI = "mongodb+srv://vgangula:4hQkpmzfPrkdrEV2@cleanenergy.awuux.mongodb.net/";

// Connect to MongoDB
mongoose.connect(MONGO_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => console.log("Connected to MongoDB"))
.catch(err => console.error("Error connecting to MongoDB:", err));

// Define a schema for the collection
const MineralSchema = new mongoose.Schema({
  Entity: String,
  Code: String,
  Year: Number,
  ShareOfGlobalProduction: Number
});

// Create a model for the collection
const Mineral = mongoose.model("Mineral", MineralSchema);

// Path to your CSV file
const csvFilePath = "./data/data.csv";

// Convert CSV to JSON and upload to MongoDB
csvtojson()
  .fromFile(csvFilePath)
  .then(async (jsonArray) => {
    try {
      // Preprocess the JSON array to parse `Tonnes` as float
      const processedData = jsonArray.map((row) => ({
        ...row,
        ShareOfGlobalProduction: parseFloat(row['share of global production|Bauxite|Mine|tonnes'])
      }));

      console.log(processedData);

      // Insert JSON records into MongoDB
      const result = await Mineral.insertMany(processedData);
      console.log("Data successfully uploaded to MongoDB:", result);
    } catch (error) {
      console.error("Error uploading data:", error);
    } finally {
      mongoose.connection.close(); // Close the connection after upload
    }
  })
  .catch((error) => {
    console.error("Error reading CSV file:", error);
  });
