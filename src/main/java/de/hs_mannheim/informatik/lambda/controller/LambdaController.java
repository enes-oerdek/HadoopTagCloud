package de.hs_mannheim.informatik.lambda.controller;

import java.awt.Color;
import java.awt.Dimension;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.CircleBackground;
import com.kennycason.kumo.font.scale.SqrtFontScalar;
import com.kennycason.kumo.nlp.FrequencyAnalyzer;
import com.kennycason.kumo.palette.ColorPalette;

@Controller
public class LambdaController {
	public final static String CLOUD_PATH = "tagclouds/";

	@GetMapping("/upload")
	public String forward(Model model) {
		model.addAttribute("files", listTagClouds());

		return "upload";
	}

	@PostMapping("/upload")
	public String handleFileUpload(@RequestParam("file") MultipartFile file, Model model) {

		
		try {
			
			PrintWriter out = new PrintWriter("resources/tagcloudfiles/"+file.getOriginalFilename());
			out.println(new String(file.getBytes()));
			out.close();
			
			model.addAttribute("message", "Datei erfolgreich hochgeladen: " + file.getOriginalFilename());
			createTagCloud(file.getOriginalFilename(), new String(file.getBytes()));

			new Thread(() -> {
				System.out.println("Hello");
			    try {
					TagCloud.GlobalJob();
					TagCloud.LocalJob(file.getOriginalFilename());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}).start();
			model.addAttribute("files", listTagClouds());
		} catch (IOException e) {
			e.printStackTrace();
			model.addAttribute("message", "Da gab es einen Fehler: " + e.getMessage());
		}

		return "upload";
	}

	private String[] listTagClouds() {
		File[] files = new File(CLOUD_PATH).listFiles();
		String[] clouds = new String[files.length];

		for (int i = 0; i < files.length; i++) {
			String name = files[i].getName();
			if (files[i].getName().endsWith(".png"))
				clouds[i] = CLOUD_PATH + name;
		}

		return clouds;
	}

	private void createTagCloud(String filename, String inhalt) throws IOException {
		final FrequencyAnalyzer frequencyAnalyzer = new FrequencyAnalyzer();
		frequencyAnalyzer.setWordFrequenciesToReturn(300);
		frequencyAnalyzer.setMinWordLength(4);

		List<String> texts = new ArrayList<>();
		texts.add(inhalt);
		final List<WordFrequency> wordFrequencies = frequencyAnalyzer.load(texts);

		final Dimension dimension = new Dimension(600, 600);
		final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
		wordCloud.setPadding(2);
		wordCloud.setBackground(new CircleBackground(300));
		wordCloud.setColorPalette(new ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
		wordCloud.setFontScalar(new SqrtFontScalar(8, 50));
		wordCloud.build(wordFrequencies);
		wordCloud.writeToFile(CLOUD_PATH + filename + ".png");
	}
	

}
