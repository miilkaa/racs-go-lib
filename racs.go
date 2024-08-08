package racs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
)

type Racs struct {
	Resource string
	Dataset  string
	Headers  map[string]string
	BaseURL  string
}

// Custom errors
var (
	ErrNoUpdatesMade   = errors.New("no updates were made")
	ErrFailedDelete    = errors.New("failed to delete post")
)

// NewRacs - конструктор для создания нового объекта Racs
func NewRacs(resource, dataset string) (*Racs, error) {
	if resource == "" {
		return nil, errors.New("resource can't be empty")
	}
	if dataset == "" {
		return nil, errors.New("dataset can't be empty")
	}

	return &Racs{
		Resource: resource,
		Dataset:  dataset,
		Headers:  map[string]string{"Content-Type": "application/json"},
		BaseURL:  "https://racs.rest/v3",
	}, nil
}

func (r *Racs) CreatePost(data map[string]interface{}) (map[string]interface{}, error) {
	if data == nil {
		return nil, errors.New(`"data" is required`)
	}

	url := fmt.Sprintf("%s?resource=%s&dataset=%s", r.BaseURL, r.Resource, r.Dataset)
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	resp, err := r.makeRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (r *Racs) CreateFile(filePath string) (map[string]interface{}, error) {
	if filePath == "" {
		return nil, errors.New(`"file_path" is required`)
	}

	url := fmt.Sprintf("%s?resource=%s&dataset=%s", r.BaseURL, r.Resource, r.Dataset)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := io.MultiWriter(body)

	if _, err := io.Copy(writer, file); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "multipart/form-data")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func (r *Racs) ReadPostByID(postID string) (map[string]interface{}, error) {
	if postID == "" {
		return nil, errors.New(`"post_id" is required`)
	}

	url := fmt.Sprintf("%s/%s?resource=%s&dataset=%s", r.BaseURL, postID, r.Resource, r.Dataset)
	resp, err := r.makeRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (r *Racs) ReadPostByFilter(filterData interface{}, sort interface{}, limit int) (map[string]interface{}, error) {
	if filterData == nil {
		filterData = make(map[string]interface{})
	}
	if sort == nil {
		sort = map[string]int{"_created": -1}
	}
	if limit == 0 {
		limit = 1
	}

	url := fmt.Sprintf("%s/get?resource=%s&dataset=%s", r.BaseURL, r.Resource, r.Dataset)
	payload, err := json.Marshal(map[string]interface{}{
		"filter": filterData,
		"sort":   sort,
		"limit":  limit,
	})
	if err != nil {
		return nil, err
	}

	resp, err := r.makeRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (r *Racs) ReadFileByID(postID string) (map[string]interface{}, error) {
	if postID == "" {
		return nil, errors.New(`"post_id" is required`)
	}

	url := fmt.Sprintf("%s/file/%s?resource=%s&dataset=%s", r.BaseURL, postID, r.Resource, r.Dataset)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/octet-stream")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func (r *Racs) UpdatePostByID(postID string, updateOptions map[string]interface{}) (map[string]interface{}, error) {
	if postID == "" {
		return nil, errors.New(`"post_id" is required`)
	}
	if updateOptions == nil {
		return nil, errors.New(`"update_options" is required`)
	}

	url := fmt.Sprintf("%s/%s?resource=%s&dataset=%s", r.BaseURL, postID, r.Resource, r.Dataset)
	payload, err := json.Marshal(map[string]interface{}{
		"update": map[string]interface{}{
			"$set": updateOptions,
		},
	})
	if err != nil {
		return nil, err
	}

	resp, err := r.makeRequest("PATCH", url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}

	if resp["matchedCount"].(float64) == 0 && resp["modifiedCount"].(float64) == 0 {
		return nil, ErrNoUpdatesMade
	}

	if resp["matchedCount"].(float64) > resp["modifiedCount"].(float64) {
		fmt.Println("Warning: matchedCount is greater than modifiedCount.")
	}

	return resp, nil
}

func (r *Racs) UpdatePostByFilter(filterData, updateOptions map[string]interface{}) (map[string]interface{}, error) {
	if filterData == nil {
		return nil, errors.New(`"filter_data" is required`)
	}
	if updateOptions == nil {
		return nil, errors.New(`"update_options" is required`)
	}

	url := fmt.Sprintf("%s?resource=%s&dataset=%s", r.BaseURL, r.Resource, r.Dataset)
	payload, err := json.Marshal(map[string]interface{}{
		"filter": filterData,
		"update": map[string]interface{}{
			"$set": updateOptions,
		},
	})
	if err != nil {
		return nil, err
	}

	resp, err := r.makeRequest("PATCH", url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}

	if resp["matchedCount"].(float64) == 0 && resp["modifiedCount"].(float64) == 0 {
		return nil, ErrNoUpdatesMade
	}

	if resp["matchedCount"].(float64) > resp["modifiedCount"].(float64) {
		fmt.Println("Warning: matchedCount is greater than modifiedCount.")
	}

	return resp, nil
}

func (r *Racs) DeletePostByID(postID string) (map[string]interface{}, error) {
	if postID == "" {
		return nil, errors.New(`"post_id" is required`)
	}

	url := fmt.Sprintf("%s/%s?resource=%s&dataset=%s", r.BaseURL, postID, r.Resource, r.Dataset)
	resp, err := r.makeRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	if resp["deletedCount"].(float64) == 0 {
		return nil, ErrFailedDelete
	}

	return resp, nil
}

func (r *Racs) DeletePostByFilter(filterData map[string]interface{}) (map[string]interface{}, error) {
	if filterData == nil {
		return nil, errors.New(`"filter_data" is required`)
	}

	url := fmt.Sprintf("%s?resource=%s&dataset=%s", r.BaseURL, r.Resource, r.Dataset)
	payload, err := json.Marshal(map[string]interface{}{
		"filter": filterData,
	})
	if err != nil {
		return nil, err
	}

	resp, err := r.makeRequest("DELETE", url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}

	if resp["deletedCount"].(float64) == 0 {
		return nil, ErrFailedDelete
	}

	return resp, nil
}

func (r *Racs) makeRequest(method, url string, body io.Reader) (map[string]interface{}, error) {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	for key, value := range r.Headers {
		req.Header.Set(key, value)
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}
