package storageaccount

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type storageaccount struct {
	client *azblob.Client
	err    error
}

var blobInstance *storageaccount

func NewBlob() *storageaccount {
	if blobInstance == nil {
		blobInstance = &storageaccount{}

		// Upload the file to Azure Blob Storage
		accountName, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT_NAME")
		if !ok {
			panic("AZURE_STORAGE_ACCOUNT_NAME could not be found")
		}
		accountKey, ok := os.LookupEnv("AZURE_STORAGE_ACCOUNT_KEY")
		if !ok {
			panic("AZURE_STORAGE_ACCOUNT_KEY could not be found")
		}
		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)

		// Create a credential using Account Name and Account Key
		cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		handleError(err)

		// Create a client to interact with the blob service
		blobInstance.client, blobInstance.err = azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
		handleError(err)

		fmt.Println("Storage Account Connected...")
	}

	return blobInstance
}

func (blobInstance storageaccount) UploadFile(containerName string, path string, fileName string, jsonData []byte) error {
	// Set up the file to upload
	err := os.WriteFile(fileName, jsonData, 0666)
	handleError(err)

	// Open the file to upload
	fileHandler, err := os.Open(fileName)
	handleError(err)

	// Delete the local file if required
	defer func(file string) {
		err = os.Remove(fileName)
		handleError(err)
	}(fileName)

	// Close the file after it is no longer required
	defer func(file *os.File) {
		err = file.Close()
		handleError(err)
	}(fileHandler)

	// Upload the file to a block blob
	_, err = blobInstance.client.UploadFile(context.TODO(), containerName, path+fileName, fileHandler,
		&azblob.UploadFileOptions{
			BlockSize:   int64(1024), // Block size in bytes
			Concurrency: uint16(3),   // Number of concurrent uploads
			Progress: func(bytesTransferred int64) {
				fmt.Printf("Uploaded %s: %d bytes\n", fileName, bytesTransferred)
			},
		})
	if err != nil {
		return err
	}

	return nil
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
